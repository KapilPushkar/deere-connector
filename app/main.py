from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
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


##
# Initialize logging
setup_logging()
logger = get_logger(__name__)

app = FastAPI(title="Deere Connector API")

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
app = FastAPI(title="AgriCapture JDOC Integration")

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

@app.get("/api/fields/{field_id}/operations")
async def get_field_operations(
    field_id: str,
    org_id: str = Query(...),
    farmer_id: str = Query(...),
    start_date: Optional[str] = Query(None, description="ISO format: YYYY-MM-DDTHH:MM:SS.000Z"),
    end_date: Optional[str] = Query(None, description="ISO format: YYYY-MM-DDTHH:MM:SS.000Z")
):
    """
    Get operations for a specific field within a date range
    
    Path params:
        field_id: Field ID
    Query params:
        org_id: Organization ID (required)
        farmer_id: Farmer identifier (required)
        start_date: Start date (optional, e.g., '2020-01-01T00:00:00.000Z')
        end_date: End date (optional, e.g., '2025-01-01T00:00:00.000Z')
        
    Returns:
        List of field operations
        
    Example:
        GET /api/fields/abc123/operations?org_id=12345&farmer_id=farmer1&start_date=2020-01-01T00:00:00.000Z&end_date=2025-01-01T00:00:00.000Z
    """
    try:
        operations = await jdoc_client.get_field_operations(
            farmer_id, org_id, field_id, start_date, end_date
        )
        return {"operations": operations, "count": len(operations)}
    except Exception as e:
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
    Get NORMALIZED field operations for a specific field within a date range
    
    This endpoint returns operations in AgriCapture's clean format with:
    - Operation type clearly identified (PLANTING, HARVEST, TILLAGE, FERTILIZER)
    - Key fields extracted: crop, product, amount, rate, area, date
    - Much easier to use for analytics than raw JDOC response
    
    Path params:
        field_id: Field ID
    Query params:
        org_id: Organization ID (required)
        farmer_id: Farmer identifier (required)
        start_date: Start date (optional, e.g., '2020-01-01T00:00:00.000Z')
        end_date: End date (optional, e.g., '2025-01-01T00:00:00.000Z')
        
    Returns:
        List of normalized field operations
        
    Example:
        GET /api/fields/abc123/operations/normalized
            ?org_id=12345
            &farmer_id=farmer1
            &start_date=2020-01-01T00:00:00.000Z
            &end_date=2025-01-01T00:00:00.000Z
    """
    try:
        # First, get the raw operations from JDOC
        raw_operations = await jdoc_client.get_field_operations(
            farmer_id, org_id, field_id, start_date, end_date
        )
        
        # Get field name from a separate call (we'll use field_id as fallback)
        field_name = field_id  # Fallback
        try:
            fields = await jdoc_client.get_fields(farmer_id, org_id, include_boundaries=False)
            for field in fields:
                if field.get("id") == field_id:
                    field_name = field.get("name", field_id)
                    break
        except:
            pass  # If this fails, just use field_id as name
        
        # Get org name (we already have it from organizations call, but for now use as fallback)
        org_name = org_id  # Fallback
        
        # Transform each raw operation into normalized format
        from app.jdoc_api import normalize_operation
        
        normalized_ops = []
        for raw_op in raw_operations:
            try:
                normalized = normalize_operation(
                    raw_op, 
                    field_id=field_id,
                    field_name=field_name,
                    org_id=org_id,
                    org_name=org_name
                )
                normalized_ops.append(normalized)
            except Exception as e:
                # Log error but continue processing other operations
                print(f"Error normalizing operation {raw_op.get('id')}: {e}")
                continue
        
        return {
            "operations": normalized_ops,
            "count": len(normalized_ops),
            "note": "Normalized format - easier to use for analytics"
        }
        
    except Exception as e:
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
