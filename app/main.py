from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from typing import Optional
import secrets
from .config import settings
from .auth import auth
from .database import db
from .jdoc_api import jdoc_client
from .models import NormalizedOperation


import logging
import uuid
from datetime import datetime
from fastapi import FastAPI, Request
from app.logging_config import setup_logging, get_logger
from app.s3_storage import save_deere_data_to_s3, list_s3_files

import sqlite3
import io
import csv
import pathlib



##
# Initialize logging
setup_logging()
logger = get_logger(__name__)

app = FastAPI(title="AgriCapture JDOC Integration")

# Middleware to add request ID
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    # Log request start
    logger.info(
        f"Request started: {request.method} {request.url.path}",
        extra={"extra": {"request_id": request_id}}
    )
    
    try:
        response = await call_next(request)
        logger.info(
            f"Request completed: {request.method} {request.url.path} - {response.status_code}",
            extra={"extra": {"request_id": request_id}}
        )
        return response
    except Exception as e:
        logger.error(
            f"Request failed: {str(e)}",
            extra={"extra": {"request_id": request_id}},
            exc_info=True
        )
        raise

# Health check endpoint (required by Docker)
@app.get("/health")
async def health_check():
    logger.info("Health check requested")
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat()
    }

##


templates = Jinja2Templates(directory="templates")

# In-memory state storage (in production, use Redis or database)
oauth_states = {}

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page"""
    return templates.TemplateResponse("landing.html", {
        "request": request,
        "base_url": settings.BASE_URL
    })

@app.get("/auth/login")
async def login(farmer_id: Optional[str] = Query(None)):
    """
    Initiate OAuth flow - farmer clicks this link
    
    Query params:
        farmer_id: Optional identifier for the farmer
    """
    # Generate state for CSRF protection
    state = secrets.token_urlsafe(32)
    oauth_states[state] = {"farmer_id": farmer_id or "anonymous"}
    
    # Generate authorization URL
    auth_url, _ = auth.generate_authorization_url(state)
    
    return RedirectResponse(url=auth_url)

@app.get("/auth/callback")
async def callback(
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None)
):
    """
    OAuth callback endpoint - John Deere redirects here after authorization
    """
    # Handle errors
    if error:
        return JSONResponse(
            status_code=400,
            content={"error": error, "message": "Authorization failed"}
        )
    
    # Verify state (CSRF protection)
    if not state or state not in oauth_states:
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    
    farmer_info = oauth_states.pop(state)
    farmer_id = farmer_info.get("farmer_id")
    
    if not code:
        raise HTTPException(status_code=400, detail="No authorization code received")
    
    try:
        # Exchange code for tokens
        token_data = await auth.exchange_code_for_token(code)
        
        # Save tokens to database
        db.save_token(farmer_id, token_data)
        
        # Check if user needs to enable organization connections
        connections_url = await jdoc_client.check_connections_needed(farmer_id)
        
        if connections_url:
            # Redirect to John Deere Connections page with return URL
            redirect_uri = f"{settings.BASE_URL}/auth/connected"
            full_connections_url = f"{connections_url}?redirect_uri={redirect_uri}"
            return RedirectResponse(url=full_connections_url)
        
        # If no connections needed, redirect to success page
        return RedirectResponse(url=f"/auth/success?farmer_id={farmer_id}")
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "message": "Failed to complete authorization"}
        )

@app.get("/auth/connected")
async def connected():
    """User is redirected here after enabling organization connections"""
    return JSONResponse({
        "status": "success",
        "message": "Organizations connected successfully",
        "next_steps": "You can now access your field data through AgriCapture"
    })

@app.get("/auth/success")
async def success(farmer_id: str = Query(...)):
    """Success page after complete authentication"""
    return JSONResponse({
        "status": "success",
        "message": "Authentication completed successfully",
        "farmer_id": farmer_id,
        "next_steps": "AgriCapture will now automatically sync your field data"
    })



@app.get("/admin", response_class=HTMLResponse)
async def admin_ui():
    """
    Serve the admin dashboard UI.
    """
    base_dir = pathlib.Path(__file__).resolve().parent.parent  # /opt/deere-connector
    index_path = base_dir / "frontend" / "index.html"
    try:
        html = index_path.read_text(encoding="utf-8")
    except Exception:
        return HTMLResponse("<h1>Admin UI not found</h1>", status_code=500)
    return HTMLResponse(content=html, status_code=200)


base_dir = pathlib.Path(__file__).resolve().parent.parent  # /opt/deere-connector

app.mount(
    "/admin-static",
    StaticFiles(directory=str(base_dir / "frontend" / "assets")),
    name="admin-static",
)




# ============================================
# API ENDPOINTS
# ============================================

@app.get("/api/organizations")
async def list_organizations(farmer_id: str = Query(...)):
    """
    List all connected organizations (growers) for a farmer
    
    Query params:
        farmer_id: Farmer identifier
        
    Returns:
        List of organizations
    """
    try:
        organizations = await jdoc_client.get_organizations(farmer_id)
        return {"organizations": organizations, "count": len(organizations)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/sync/organizations")
async def sync_organizations_to_db(farmer_id: str = Query(...)):
    """
    Fetch organizations from Deere and store them in SQLite 'organizations' table.
    """
    try:
        orgs = await jdoc_client.get_organizations(farmer_id)
        for org in orgs:
            db.upsert_organization(farmer_id, org)

        return {
            "status": "success",
            "farmer_id": farmer_id,
            "organizations_synced": len(orgs)
        }
    except Exception as e:
        logger.error(f"Failed to sync organizations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/organizations/{org_id}/fields")
async def list_fields(org_id: str, farmer_id: str = Query(...)):
    """
    List all fields with active boundaries for an organization
    
    Path params:
        org_id: Organization ID
    Query params:
        farmer_id: Farmer identifier
        
    Returns:
        List of fields with boundaries
    """
    try:
        fields = await jdoc_client.get_fields(farmer_id, org_id, include_boundaries=True)
        return {"fields": fields, "count": len(fields)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/sync/organizations/{org_id}/fields")
async def sync_fields_to_db(
    org_id: str,
    farmer_id: str = Query(...)
):
    """
    Fetch fields with boundaries from Deere and store them in SQLite 'fields' table.
    """
    try:
        fields = await jdoc_client.get_fields(
            farmer_id, org_id, include_boundaries=True
        )
        for field in fields:
            db.upsert_field(org_id, field)

        return {
            "status": "success",
            "farmer_id": farmer_id,
            "org_id": org_id,
            "fields_synced": len(fields)
        }
    except Exception as e:
        logger.error(f"Failed to sync fields: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/api/fields/{field_id}/operations")
async def get_field_operations(
    field_id: str,
    org_id: str = Query(...),
    farmer_id: str = Query(...),
    start_date: Optional[str] = Query(None, description="ISO format: YYYY-MM-DDTHH:MM:SS.000Z"),
    end_date: Optional[str] = Query(None, description="ISO format: YYYY-MM-DDTHH:MM:SS.000Z")
):
    """
    Get operations for a specific field within a date range and
    store the raw JSON into SQLite.
    """
    try:
        # Existing Deere call, unchanged in terms of parameters
        operations = await jdoc_client.get_field_operations(
            farmer_id, org_id, field_id, start_date, end_date
        )

        # NEW: persist each operation as raw JSON
        for op in operations:
            db.upsert_raw_operation(
                org_id=org_id,
                field_id=field_id,
                operation=op,
            )

        return {
            "status": "success",
            "farmer_id": farmer_id,
            "org_id": org_id,
            "field_id": field_id,
            "count": len(operations),
            "operations": operations,
        }
    except Exception as e:
        logger.error(f"Failed to fetch/store operations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    

@app.get("/api/fields/{field_id}/operations/normalized")
async def get_normalized_field_operations(
    field_id: str,
    org_id: str = Query(...),
    farmer_id: str = Query(...),
    start_date: Optional[str] = Query(None, description="ISO format: YYYY-MM-DDTHH:MM:SS.000Z"),
    end_date: Optional[str] = Query(None, description="ISO format: YYYY-MM-DDTHH:MM:SS.000Z")
):
    """
    Get NORMALIZED field operations for a specific field within a date range.
    Also stores raw and normalized operations in SQLite.
    """
    try:
        # 1) Get the raw operations from JDOC
        raw_operations = await jdoc_client.get_field_operations(
            farmer_id, org_id, field_id, start_date, end_date
        )

        # 2) Store raw JSON in operations_raw
        for raw_op in raw_operations:
            try:
                db.upsert_raw_operation(
                    org_id=org_id,
                    field_id=field_id,
                    operation=raw_op,
                )
            except Exception as e:
                logger.error(f"Error upserting raw operation {raw_op.get('id')}: {e}", exc_info=True)

        # 3) Get field name (same as before)
        field_name = field_id  # Fallback
        try:
            fields = await jdoc_client.get_fields(farmer_id, org_id, include_boundaries=False)
            for field in fields:
                if field.get("id") == field_id:
                    field_name = field.get("name", field_id)
                    break
        except Exception:
            pass

        org_name = org_id

        # 4) Normalize using your existing normalize_operation

        from app.jdoc_api import normalize_operation

        normalized_ops = []
        for raw_op in raw_operations:
            try:
                normalized_model = normalize_operation(
                    raw_op,
                    field_id=field_id,
                    field_name=field_name,
                    org_id=org_id,
                    org_name=org_name,
                )
                # Pydantic v2: model_dump(), v1: dict()
                if hasattr(normalized_model, "model_dump"):
                    normalized = normalized_model.model_dump()
                else:
                    normalized = normalized_model.dict()
                normalized_ops.append(normalized)
            except Exception as e:
                logger.error(
                    f"Error normalizing operation {getattr(raw_op, 'id', None)}: {e}",
                    exc_info=True,
                )
                continue


        # 5) ADAPT normalized dicts to DB schema before insert
        db_rows = []
        for n in normalized_ops:
            raw = n.get("raw_jdoc_data") or {}
            op_id = raw.get("id") or f"{field_id}-{n.get('date')}"

            op_date = n.get("date")
            operation_date = op_date
            start_time = op_date
            end_time = op_date

            db_rows.append({
                "operation_id": op_id,
                "field_id": n.get("field_id"),
                "org_id": n.get("org_id"),
                "operation_type": n.get("operation_type"),
                "operation_date": operation_date,
                "start_time": start_time,
                "end_time": end_time,
                "crop_name": n.get("crop_name"),
                "product_name": n.get("product_name"),
                "product_category": None,
                "rate_value": n.get("rate"),
                "rate_unit": n.get("rate_unit"),
                "total_amount": n.get("amount"),
                "total_amount_unit": None,
                "area_ha": n.get("area"),
                "equipment_name": None,
                "notes": None,
            })

        # 6) Insert into operations_normalized
        if db_rows:
            try:
                db.insert_normalized_operations(
                    org_id=org_id,
                    field_id=field_id,
                    normalized_ops=db_rows,
                )
            except Exception as e:
                logger.error(f"Error inserting normalized operations: {e}", exc_info=True)

        return {
            "operations": normalized_ops,
            "count": len(normalized_ops),
            "note": "Normalized format - easier to use for analytics",
        }

    except Exception as e:
        logger.error(f"Failed to fetch/normalize/store operations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


    
@app.get("/api/fields/{field_id}/operations/sync")
async def get_field_operations_with_sync(
    field_id: str,
    org_id: str = Query(...),
    farmer_id: str = Query(...),
    mode: str = Query("full_history", description="'full_history' or 'incremental'"),
    lookback_years: int = Query(5, description="Years of history for full_history mode"),
    end_date: Optional[str] = Query(None, description="End date for manual override (ISO format)")
):
    """
    Get field operations with intelligent sync modes
    
    Supports two modes:
    - full_history: Pull all operations for the last N years (default 5)
    - incremental: Pull only operations since the last successful sync
    
    Path params:
        field_id: Field ID
    Query params:
        org_id: Organization ID (required)
        farmer_id: Farmer identifier (required)
        mode: "full_history" or "incremental" (default: full_history)
        lookback_years: How many years for full_history mode (default: 5)
        end_date: Optional override for end_date (ISO format)
        
    Returns:
        Normalized operations + sync state info
        
    Example (full history - 5 years):
        GET /api/fields/abc123/operations/sync
            ?org_id=12345
            &farmer_id=farmer1
            &mode=full_history
            &lookback_years=5
            
    Example (incremental - since last sync):
        GET /api/fields/abc123/operations/sync
            ?org_id=12345
            &farmer_id=farmer1
            &mode=incremental
    """
    from datetime import datetime, timedelta
    from app.jdoc_api import normalize_operation
    
    try:
        # Determine date range based on mode
        if mode == "incremental":
            # Get the last sync state for this field
            sync_state = db.get_sync_state(farmer_id, org_id, field_id)
            
            if not sync_state or not sync_state.get('last_synced_at'):
                # No previous sync, fall back to full_history with 5 years
                return JSONResponse({
                    "warning": "No previous sync found for this field, falling back to full_history (5 years)",
                    "mode_used": "full_history",
                    "note": "Run this again with mode=incremental next time after this completes"
                }, status_code=202)  # 202 Accepted - operation started
            
            # Use last sync end date as new start date
            start_date = sync_state.get('last_sync_end_date')
            if end_date is None:
                end_date = datetime.now().isoformat() + "Z"
            
            sync_mode = "incremental"
        
        else:  # full_history mode
            # Calculate start date as N years ago
            start_date = (datetime.now() - timedelta(days=365*lookback_years)).isoformat() + "Z"
            if end_date is None:
                end_date = datetime.now().isoformat() + "Z"
            
            sync_mode = "full_history"
        
        # Fetch raw operations from JDOC
        raw_operations = await jdoc_client.get_field_operations(
            farmer_id, org_id, field_id, start_date, end_date
        )
        
        # Get field name
        field_name = field_id
        try:
            fields = await jdoc_client.get_fields(farmer_id, org_id, include_boundaries=False)
            for field in fields:
                if field.get("id") == field_id:
                    field_name = field.get("name", field_id)
                    break
        except:
            pass
        
        # Transform to normalized format
        normalized_ops = []
        for raw_op in raw_operations:
            try:
                normalized = normalize_operation(
                    raw_op,
                    field_id=field_id,
                    field_name=field_name,
                    org_id=org_id,
                    org_name=org_id  # Fallback to org_id
                )
                normalized_ops.append(normalized)
            except Exception as e:
                print(f"Error normalizing operation: {e}")
                continue
        
        # Save sync state for future incremental pulls
        try:
            db.save_sync_state(
                farmer_id=farmer_id,
                org_id=org_id,
                field_id=field_id,
                field_name=field_name,
                sync_mode=sync_mode,
                start_date=start_date,
                end_date=end_date
            )
        except Exception as e:
            print(f"Warning: Could not save sync state: {e}")
        
        return {
            "operations": normalized_ops,
            "count": len(normalized_ops),
            "sync_info": {
                "mode": sync_mode,
                "farmer_id": farmer_id,
                "org_id": org_id,
                "field_id": field_id,
                "start_date": start_date,
                "end_date": end_date,
                "synced_at": datetime.now().isoformat()
            },
            "note": "Operations are normalized and sync state has been saved for next incremental pull"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


VALID_TABLES = {
    "organizations",
    "fields",
    "operations_raw",
    "operations_normalized",
    "field_sync_state",
    "connected_organizations",
    "user_tokens",
}


@app.get("/admin/tables/{table_name}")
async def view_table(table_name: str):
    if table_name not in VALID_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")

    rows = db.fetch_all_rows(table_name)
    return {
        "table": table_name,
        "count": len(rows),
        "rows": rows,
    }


@app.get("/admin/tables/{table_name}/download")
async def download_table_csv(table_name: str):
    if table_name not in VALID_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")

    cols, rows = db.fetch_all_rows_raw(table_name)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(cols)
    writer.writerows(rows)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{table_name}.csv"',
        },
    )




@app.get("/admin/operations/normalized")
async def list_normalized_operations(
    org_id: Optional[str] = Query(None),
    field_id: Optional[str] = Query(None),
):
    """
    Flat view of normalized operations, joined with org & field names.
    Optional filters: org_id, field_id.
    """
    rows = db.fetch_all_normalized_operations(org_id=org_id, field_id=field_id)
    return {
        "count": len(rows),
        "operations": rows,
    }


@app.get("/admin/operations/normalized/download")
async def download_normalized_operations_csv(
    org_id: Optional[str] = Query(None),
    field_id: Optional[str] = Query(None),
):
    """
    Download all normalized operations (optionally filtered) as CSV.
    """
    # Reuse the DB helper but we need columns + rows
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()

    base_sql = """
        SELECT
          o.operation_id,
          o.org_id,
          org.name AS org_name,
          o.field_id,
          f.name AS field_name,
          o.operation_type,
          o.operation_date,
          o.start_time,
          o.end_time,
          o.crop_name,
          o.product_name,
          o.product_category,
          o.rate_value,
          o.rate_unit,
          o.total_amount,
          o.total_amount_unit,
          o.area_ha,
          o.equipment_name,
          o.notes
        FROM operations_normalized o
        LEFT JOIN fields f
          ON o.field_id = f.field_id
        LEFT JOIN organizations org
          ON o.org_id = org.org_id
    """

    conditions = []
    params = []

    if org_id:
        conditions.append("o.org_id = ?")
        params.append(org_id)
    if field_id:
        conditions.append("o.field_id = ?")
        params.append(field_id)

    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)

    cursor.execute(base_sql, params)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(cols)
    writer.writerows(rows)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": 'attachment; filename="operations_normalized.csv"',
        },
    )



from typing import Optional

@app.post("/admin/sync/farmer")
async def sync_farmer_data(
    farmer_id: str = Query(..., description="AgriCapture farmer id / JDOC user_id"),
    org_id: Optional[str] = Query(None, description="Optional: limit sync to one organization"),
    start_date: Optional[str] = Query(None, description="Optional: ISO start date for operations"),
    end_date: Optional[str] = Query(None, description="Optional: ISO end date for operations"),
):
    """
    Walk Deere hierarchy for a farmer and populate:
    - organizations
    - fields (per organization)
    - operations_raw
    - operations_normalized
    """
    synced_orgs = 0
    synced_fields = 0
    synced_ops = 0

    # 1) Get organizations (either all or a specific one)
    orgs = await jdoc_client.get_organizations(farmer_id)

    if org_id:
        orgs = [o for o in orgs if o.get("id") == org_id]

    for org in orgs:
        oid = org.get("id")
        if not oid:
            continue
        synced_orgs += 1

        # Ensure org is in DB (if get_organizations doesn't already upsert)
        try:
            db.upsert_organization(farmer_id, org)
        except Exception as e:
            logger.error(f"Error upserting organization {oid}: {e}", exc_info=True)

        # 2) Fetch fields for this org
        try:
            fields = await jdoc_client.get_fields(farmer_id, oid, include_boundaries=True)
        except Exception as e:
            logger.error(f"Error fetching fields for org {oid}: {e}", exc_info=True)
            continue

        for field in fields:
            fid = field.get("id")
            if not fid:
                continue

            # Save field in DB
            try:
                db.upsert_field(oid, field)
                synced_fields += 1
            except Exception as e:
                logger.error(f"Error upserting field {fid} for org {oid}: {e}", exc_info=True)

            # 3) Fetch raw operations for this field
            try:
                raw_ops = await jdoc_client.get_field_operations(
                    farmer_id, oid, fid, start_date, end_date
                )
            except Exception as e:
                logger.error(f"Error fetching operations for field {fid}, org {oid}: {e}", exc_info=True)
                continue

            from app.jdoc_api import normalize_operation

            # 3a) Store raw & normalized operations
            normalized_ops = []
            for raw_op in raw_ops:
                try:
                    # raw → operations_raw
                    db.upsert_raw_operation(org_id=oid, field_id=fid, operation=raw_op)

                    # normalize → NormalizedOperation model
                    norm_model = normalize_operation(
                        raw_op,
                        field_id=fid,
                        field_name=field.get("name", fid),
                        org_id=oid,
                        org_name=org.get("name", oid),
                    )

                    # Pydantic v1/v2
                    if hasattr(norm_model, "model_dump"):
                        norm_dict = norm_model.model_dump()
                    else:
                        norm_dict = norm_model.dict()

                    normalized_ops.append(norm_dict)
                except Exception as e:
                    logger.error(f"Error normalizing/storing operation: {e}", exc_info=True)
                    continue

            if normalized_ops:
                try:
                    # adapt to DB schema (same mapping you use in /operations/normalized)
                    db_rows = []
                    for n in normalized_ops:
                        raw = n.get("raw_jdoc_data") or {}
                        op_id = raw.get("id") or f"{fid}-{n.get('date')}"
                        op_date = n.get("date")

                        db_rows.append({
                            "operation_id": op_id,
                            "field_id": n.get("field_id"),
                            "org_id": n.get("org_id"),
                            "operation_type": n.get("operation_type"),
                            "operation_date": op_date,
                            "start_time": op_date,
                            "end_time": op_date,
                            "crop_name": n.get("crop_name"),
                            "product_name": n.get("product_name"),
                            "product_category": None,
                            "rate_value": n.get("rate"),
                            "rate_unit": n.get("rate_unit"),
                            "total_amount": n.get("amount"),
                            "total_amount_unit": None,
                            "area_ha": n.get("area"),
                            "equipment_name": None,
                            "notes": None,
                        })

                    db.insert_normalized_operations(
                        org_id=oid,
                        field_id=fid,
                        normalized_ops=db_rows,
                    )
                    synced_ops += len(db_rows)
                except Exception as e:
                    logger.error(f"Error inserting normalized operations for field {fid}: {e}", exc_info=True)

    return {
        "status": "success",
        "farmer_id": farmer_id,
        "org_filter": org_id,
        "synced_organizations": synced_orgs,
        "synced_fields": synced_fields,
        "synced_operations": synced_ops,
        "note": "Data populated into organizations, fields, operations_raw, operations_normalized",
    }




@app.get("/admin/dashboard/summary")
async def get_dashboard_summary():
    """
    Summary stats for admin overview page.
    """
    summary = db.get_dashboard_summary()

    # For now, farmers_count = distinct farmer_id in organizations
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT farmer_id) FROM organizations")
    farmers_count = cursor.fetchone()[0]
    conn.close()

    return {
        "organizations_connected": summary["organizations_count"],
        "fields_connected": summary["fields_count"],
        "total_area_ha": summary["total_area_ha"],
        "operations_count": summary["operations_count"],
        "farmers_connected": farmers_count,
    }



# ============================================
# HEALTH & MONITORING ENDPOINTS
# ============================================

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "environment": settings.ENVIRONMENT,
        "api_base": settings.api_base_url
    }

@app.get("/api/stats")
async def get_stats():
    """Get basic statistics about connected farmers and organizations"""
    # This is a simple implementation - you can expand this
    return {
        "message": "Stats endpoint - implement based on your needs",
        "connected_farmers": 0,  # Query database for count
        "total_organizations": 0,  # Query database for count
        "last_sync": None  # Track last successful sync
    }

@app.get("/api/debug/sync-states")
async def view_sync_states(farmer_id: str = Query(...)):
    """
    View all sync states for a farmer (for debugging and monitoring)
    
    Query params:
        farmer_id: Farmer identifier
        
    Returns:
        All fields and their last sync history
    """
    try:
        sync_states = db.get_all_sync_states(farmer_id)
        return {
            "farmer_id": farmer_id,
            "sync_states": sync_states,
            "count": len(sync_states)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/api/farmers/{farmer_id}/snapshot")
async def get_farmer_snapshot(
    farmer_id: str,
    mode: str = Query("full_history", description="'full_history' or 'incremental'"),
    lookback_years: int = Query(5, description="Years of history for full_history mode")
):
    """
    Get complete Leaf-like snapshot of farmer's data across all organizations and fields
    
    This is a high-level "job" endpoint that:
    1. Lists all organizations for the farmer
    2. For each organization, fetches all fields with boundaries
    3. For each field, fetches normalized operations
    4. Returns everything in Leaf-like hierarchy format
    
    Perfect for:
    - Initial data pulls at enrollment (full_history mode)
    - End-of-season updates (incremental mode)
    - Bulk exports and analytics processing
    
    Path params:
        farmer_id: Farmer identifier
    Query params:
        mode: "full_history" or "incremental" (default: full_history)
        lookback_years: Years of history (default: 5)
        
    Returns:
        FarmerSnapshot with full hierarchy and all data
        
    Example (enrollment - 5 years):
        GET /api/farmers/farmer1/snapshot?mode=full_history&lookback_years=5
        
    Example (end of season - since last sync):
        GET /api/farmers/farmer1/snapshot?mode=incremental
    """
    from datetime import datetime, timedelta
    from app.jdoc_api import normalize_operation, build_leaf_like_hierarchy
    from app.models import FarmerSnapshot
    
    try:
        # Step 1: Get all organizations
        orgs_raw = await jdoc_client.get_organizations(farmer_id)
        
        if not orgs_raw:
            return JSONResponse(
                {"error": "No organizations found for this farmer"},
                status_code=404
            )
        
        # Step 2: For each org, fetch all fields and operations
        fields_with_boundaries = {}  # {org_id: {field_id: field_data}}
        operations_normalized = {}   # {field_id: [normalized_ops]}
        
        for org in orgs_raw:
            org_id = org.get("id")
            fields_with_boundaries[org_id] = {}
            
            try:
                # Fetch all fields for this org
                fields_raw = await jdoc_client.get_fields(farmer_id, org_id, include_boundaries=True)
                
                for field in fields_raw:
                    field_id = field.get("id")
                    fields_with_boundaries[org_id][field_id] = field
                    
                    # Determine date range based on mode
                    if mode == "incremental":
                        sync_state = db.get_sync_state(farmer_id, org_id, field_id)
                        if sync_state and sync_state.get('last_synced_at'):
                            start_date = sync_state.get('last_sync_end_date')
                            end_date = datetime.now().isoformat() + "Z"
                        else:
                            # Fall back to full history if no previous sync
                            start_date = (datetime.now() - timedelta(days=365*lookback_years)).isoformat() + "Z"
                            end_date = datetime.now().isoformat() + "Z"
                    else:  # full_history
                        start_date = (datetime.now() - timedelta(days=365*lookback_years)).isoformat() + "Z"
                        end_date = datetime.now().isoformat() + "Z"
                    
                    # Fetch operations for this field
                    try:
                        raw_ops = await jdoc_client.get_field_operations(
                            farmer_id, org_id, field_id, start_date, end_date
                        )
                        
                        # Normalize each operation
                        normalized_ops = []
                        for raw_op in raw_ops:
                            try:
                                norm_op = normalize_operation(
                                    raw_op,
                                    field_id=field_id,
                                    field_name=field.get("name", field_id),
                                    org_id=org_id,
                                    org_name=org.get("name", org_id)
                                )
                                normalized_ops.append(norm_op)
                            except Exception as e:
                                print(f"Error normalizing operation: {e}")
                                continue
                        
                        operations_normalized[field_id] = normalized_ops
                        
                        # Save sync state
                        try:
                            db.save_sync_state(
                                farmer_id=farmer_id,
                                org_id=org_id,
                                field_id=field_id,
                                field_name=field.get("name", field_id),
                                sync_mode=mode,
                                start_date=start_date,
                                end_date=end_date
                            )
                        except:
                            pass  # Continue even if sync state save fails
                        
                    except Exception as e:
                        print(f"Error fetching operations for field {field_id}: {e}")
                        operations_normalized[field_id] = []
                
            except Exception as e:
                print(f"Error fetching fields for org {org_id}: {e}")
                continue
        
        # Step 3: Build Leaf-like hierarchy
        organizations = build_leaf_like_hierarchy(
            farmer_id=farmer_id,
            organizations_raw=orgs_raw,
            fields_with_boundaries=fields_with_boundaries,
            operations_normalized=operations_normalized
        )
        
        # Step 4: Calculate totals
        total_fields = sum(len(org.get_all_fields()) for org in organizations)
        total_operations = sum(
            sum(len(field.operations) for field in org.get_all_fields())
            for org in organizations
        )
        
        # Step 5: Return snapshot
        snapshot = FarmerSnapshot(
            farmer_id=farmer_id,
            organizations=organizations,
            sync_info={
                "mode": mode,
                "lookback_years": lookback_years,
                "snapshot_generated_at": datetime.now().isoformat()
            },
            total_fields=total_fields,
            total_operations=total_operations
        )
        
        return snapshot.model_dump()
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
