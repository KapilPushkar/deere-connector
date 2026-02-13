import sqlite3
import json
from datetime import datetime
from typing import Optional, Dict, List, Tuple


class Database:
    def __init__(self, db_path: str = "agricapture.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize the database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Table to store user tokens
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT UNIQUE NOT NULL,
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                token_type TEXT,
                expires_at TIMESTAMP,
                scopes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Table to store connected organizations
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS connected_organizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                org_id TEXT NOT NULL,
                org_name TEXT,
                org_type TEXT,
                is_enabled INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, org_id)
            )
        ''')
        
        # Table to track sync history per field/farmer
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS field_sync_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                farmer_id TEXT NOT NULL,
                org_id TEXT NOT NULL,
                field_id TEXT NOT NULL,
                field_name TEXT,
                last_synced_at TIMESTAMP,
                last_sync_mode TEXT,
                last_sync_start_date TEXT,
                last_sync_end_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(farmer_id, org_id, field_id)
            )
        ''')
        
        # Normalized organizations (per Deere org)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS organizations (
                org_id TEXT PRIMARY KEY,
                farmer_id TEXT NOT NULL,
                name TEXT,
                type TEXT,
                country TEXT,
                time_zone TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Fields per organization
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fields (
                field_id TEXT PRIMARY KEY,
                org_id TEXT NOT NULL,
                name TEXT,
                external_id TEXT,
                area_ha REAL,
                geometry_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Raw Deere operations JSON (for traceability)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS operations_raw (
                operation_id TEXT PRIMARY KEY,
                field_id TEXT NOT NULL,
                org_id TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                event_start TIMESTAMP,
                event_end TIMESTAMP,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Normalized operations (analytics‑ready)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS operations_normalized (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id TEXT NOT NULL,
                field_id TEXT NOT NULL,
                org_id TEXT NOT NULL,
                operation_type TEXT,
                operation_date TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                crop_name TEXT,
                product_name TEXT,
                product_category TEXT,
                rate_value REAL,
                rate_unit TEXT,
                total_amount REAL,
                total_amount_unit TEXT,
                area_ha REAL,
                equipment_name TEXT,
                notes TEXT,
                normalized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()
    
    def save_token(self, user_id: str, token_data: Dict):
        """Save or update user tokens"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO user_tokens 
            (user_id, access_token, refresh_token, token_type, expires_at, scopes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            token_data.get('access_token'),
            token_data.get('refresh_token'),
            token_data.get('token_type'),
            token_data.get('expires_at'),
            token_data.get('scope'),
            datetime.now()
        ))
        
        conn.commit()
        conn.close()
    
    def get_token(self, user_id: str) -> Optional[Dict]:
        """Retrieve user tokens"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM user_tokens WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return dict(row)
        return None
    
    def save_organization(self, user_id: str, org_data: Dict):
        """Save connected organization"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO connected_organizations 
            (user_id, org_id, org_name, org_type, is_enabled)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            user_id,
            org_data.get('id'),
            org_data.get('name'),
            org_data.get('type'),
            1 if 'manage_connection' in str(org_data.get('links', [])) else 0
        ))
        
        conn.commit()
        conn.close()
    
    def get_organizations(self, user_id: str):
        """Get all organizations for a user"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM connected_organizations 
            WHERE user_id = ?
            ORDER BY created_at DESC
        ''', (user_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def save_sync_state(self, farmer_id: str, org_id: str, field_id: str, field_name: str, sync_mode: str, start_date: str, end_date: str):
        """Save sync history for a field"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO field_sync_state 
            (farmer_id, org_id, field_id, field_name, last_synced_at, last_sync_mode, last_sync_start_date, last_sync_end_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            farmer_id,
            org_id,
            field_id,
            field_name,
            datetime.now(),
            sync_mode,
            start_date,
            end_date,
            datetime.now()
        ))
        
        conn.commit()
        conn.close()
    
    def get_sync_state(self, farmer_id: str, org_id: str, field_id: str) -> Optional[Dict]:
        """Get the last sync info for a field"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM field_sync_state 
            WHERE farmer_id = ? AND org_id = ? AND field_id = ?
        ''', (farmer_id, org_id, field_id))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return dict(row)
        return None
    
    def get_all_sync_states(self, farmer_id: str) -> List[Dict]:
        """Get all sync states for a farmer"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM field_sync_state 
            WHERE farmer_id = ?
            ORDER BY updated_at DESC
        ''', (farmer_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]

    # ---------- NEW: Organizations & Fields ----------

    def upsert_organization(self, farmer_id: str, org_data: Dict):
        """Insert or update an organization record"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO organizations (org_id, farmer_id, name, type, country, time_zone, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(org_id) DO UPDATE SET
                farmer_id = excluded.farmer_id,
                name = excluded.name,
                type = excluded.type,
                country = excluded.country,
                time_zone = excluded.time_zone,
                updated_at = CURRENT_TIMESTAMP
        ''', (
            org_data.get('id'),
            farmer_id,
            org_data.get('name'),
            org_data.get('type'),
            org_data.get('countryCode'),
            org_data.get('timeZone')
        ))

        conn.commit()
        conn.close()

    def upsert_field(self, org_id: str, field_data: Dict):
        """Insert or update a field record"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        geometry_json = None
        if 'boundaries' in field_data:
            try:
                geometry_json = json.dumps(field_data.get('boundaries'))
            except Exception:
                geometry_json = None

        cursor.execute('''
            INSERT INTO fields (field_id, org_id, name, external_id, area_ha, geometry_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(field_id) DO UPDATE SET
                org_id = excluded.org_id,
                name = excluded.name,
                external_id = excluded.external_id,
                area_ha = excluded.area_ha,
                geometry_json = excluded.geometry_json,
                updated_at = CURRENT_TIMESTAMP
        ''', (
            field_data.get('id'),
            org_id,
            field_data.get('name'),
            field_data.get('externalId'),
            field_data.get('area', {}).get('value') if isinstance(field_data.get('area'), dict) else None,
            geometry_json
        ))

        conn.commit()
        conn.close()


    def fetch_all_normalized_operations(
        self,
        org_id: str | None = None,
        field_id: str | None = None,
    ):
        """
        Return joined normalized operations with org & field names.
        Optional filters: org_id, field_id.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
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
        rows = cursor.fetchall()
        conn.close()

        return [dict(r) for r in rows]



    def get_dashboard_summary(self):
        """
        Return basic counts and totals for the admin dashboard.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM organizations")
        orgs_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM fields")
        fields_count = cursor.fetchone()[0]

        cursor.execute("SELECT COALESCE(SUM(area_ha), 0) FROM fields")
        total_area_ha = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(*) FROM operations_normalized")
        operations_count = cursor.fetchone()[0]

        conn.close()

        return {
            "organizations_count": orgs_count,
            "fields_count": fields_count,
            "total_area_ha": total_area_ha,
            "operations_count": operations_count,
        }




    # ---------- NEW: Raw & Normalized Operations ----------

    def upsert_raw_operation(self, org_id: str, field_id: str, operation: Dict):
        """Store raw Deere operation JSON"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        op_id = operation.get('id')
        start_ts = operation.get('startTime')
        end_ts = operation.get('endTime')

        cursor.execute('''
            INSERT INTO operations_raw (operation_id, field_id, org_id, raw_json, event_start, event_end, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(operation_id) DO UPDATE SET
                field_id = excluded.field_id,
                org_id = excluded.org_id,
                raw_json = excluded.raw_json,
                event_start = excluded.event_start,
                event_end = excluded.event_end,
                ingested_at = CURRENT_TIMESTAMP
        ''', (
            op_id,
            field_id,
            org_id,
            json.dumps(operation),
            start_ts,
            end_ts
        ))

        conn.commit()
        conn.close()


    def fetch_all_rows(self, table: str):
        """Return all rows as list of dicts, for JSON view."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def fetch_all_rows_raw(self, table: str) -> Tuple[List[str], List[tuple]]:
        """Return (columns, rows) for CSV download."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return cols, rows


    def insert_normalized_operations(self, org_id: str, field_id: str, normalized_ops: List[Dict]):
        """Bulk-insert normalized operations for one field"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for op in normalized_ops:
            cursor.execute('''
                INSERT INTO operations_normalized (
                    operation_id, field_id, org_id,
                    operation_type, operation_date,
                    start_time, end_time,
                    crop_name, product_name, product_category,
                    rate_value, rate_unit,
                    total_amount, total_amount_unit,
                    area_ha, equipment_name, notes,
                    normalized_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                op.get('operation_id'),
                field_id,
                org_id,
                op.get('operation_type'),
                op.get('operation_date'),
                op.get('start_time'),
                op.get('end_time'),
                op.get('crop_name'),
                op.get('product_name'),
                op.get('product_category'),
                op.get('rate_value'),
                op.get('rate_unit'),
                op.get('total_amount'),
                op.get('total_amount_unit'),
                op.get('area_ha'),
                op.get('equipment_name'),
                op.get('notes'),
            ))

        conn.commit()
        conn.close()



# Global database instance
db = Database()
