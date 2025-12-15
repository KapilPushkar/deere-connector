import httpx
import secrets
from urllib.parse import urlencode
from datetime import datetime, timedelta
from typing import Optional, Dict
from .config import settings
from .database import db

class JohnDeereAuth:
    """Handles all OAuth 2.0 operations with John Deere"""
    
    def __init__(self):
        self.client_id = settings.CLIENT_ID
        self.client_secret = settings.CLIENT_SECRET
        self.redirect_uri = settings.REDIRECT_URI
        self.authorization_url = settings.AUTHORIZATION_URL
        self.token_url = settings.TOKEN_URL
        self.scopes = settings.SCOPES
    
    def generate_authorization_url(self, state: Optional[str] = None) -> tuple[str, str]:
        """
        Generate the URL to redirect farmers to for authorization
        
        Returns:
            (authorization_url, state) - URL to redirect to and state parameter
        """
        if not state:
            state = secrets.token_urlsafe(32)
        
        params = {
            'response_type': 'code',
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'scope': self.scopes,
            'state': state
        }
        
        auth_url = f"{self.authorization_url}?{urlencode(params)}"
        return auth_url, state
    
    async def exchange_code_for_token(self, code: str) -> Dict:
        """
        Exchange authorization code for access token
        
        Args:
            code: Authorization code from callback
            
        Returns:
            Dictionary containing access_token, refresh_token, etc.
        """
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': self.redirect_uri,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.token_url,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            if response.status_code != 200:
                raise Exception(f"Token exchange failed: {response.text}")
            
            token_data = response.json()
            
            # Calculate expiration time
            expires_in = token_data.get('expires_in', 43200)  # Default 12 hours
            token_data['expires_at'] = (datetime.now() + timedelta(seconds=expires_in)).isoformat()
            
            return token_data
    
    async def refresh_access_token(self, refresh_token: str) -> Dict:
        """
        Refresh an expired access token
        
        Args:
            refresh_token: The refresh token
            
        Returns:
            Dictionary containing new access_token
        """
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.token_url,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            if response.status_code != 200:
                raise Exception(f"Token refresh failed: {response.text}")
            
            token_data = response.json()
            
            # Calculate expiration time
            expires_in = token_data.get('expires_in', 43200)
            token_data['expires_at'] = (datetime.now() + timedelta(seconds=expires_in)).isoformat()
            
            return token_data
    
    def is_token_expired(self, token_data: Dict) -> bool:
        """Check if token is expired"""
        if not token_data.get('expires_at'):
            return True
        
        expires_at = datetime.fromisoformat(token_data['expires_at'])
        # Consider expired if less than 5 minutes remaining
        return datetime.now() >= (expires_at - timedelta(minutes=5))
    
    async def get_valid_token(self, user_id: str) -> Optional[str]:
        """
        Get a valid access token for user, refreshing if necessary
        
        Args:
            user_id: User identifier
            
        Returns:
            Valid access token or None
        """
        token_data = db.get_token(user_id)
        
        if not token_data:
            return None
        
        # If token is expired, refresh it
        if self.is_token_expired(token_data):
            if token_data.get('refresh_token'):
                try:
                    new_token_data = await self.refresh_access_token(token_data['refresh_token'])
                    # Update database with new token
                    db.save_token(user_id, new_token_data)
                    return new_token_data['access_token']
                except Exception as e:
                    print(f"Failed to refresh token: {e}")
                    return None
            else:
                return None
        
        return token_data['access_token']

# Global auth instance
auth = JohnDeereAuth()
