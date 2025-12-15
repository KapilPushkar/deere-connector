import sqlite3
import json
from datetime import datetime
from typing import Optional, Dict, List


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


# Global database instance
db = Database()
