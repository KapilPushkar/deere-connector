import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # John Deere OAuth Configuration
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")
    BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
    
    # Environment
    ENVIRONMENT = os.getenv("ENVIRONMENT", "sandbox")  # sandbox or production
    
    # OAuth URLs
    WELL_KNOWN_URL = "https://signin.johndeere.com/oauth2/aus78tnlaysMraFhC1t7/.well-known/oauth-authorization-server"
    AUTHORIZATION_URL = "https://signin.johndeere.com/oauth2/aus78tnlaysMraFhC1t7/v1/authorize"
    TOKEN_URL = "https://signin.johndeere.com/oauth2/aus78tnlaysMraFhC1t7/v1/token"
    
    # API Base URLs
    API_BASE_SANDBOX = "https://sandboxapi.deere.com/platform"
    API_BASE_PRODUCTION = "https://partnerapi.deere.com/platform"
    
    @property
    def api_base_url(self):
        return self.API_BASE_SANDBOX if self.ENVIRONMENT == "sandbox" else self.API_BASE_PRODUCTION
    
    # OAuth Scopes (what permissions we need)
    SCOPES = "org1 org2 ag2 eq1 offline_access"
    
    # AWS S3 Configuration (for data storage - NEW)
    AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "deere-connector-data-demo")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    
    # Database (kept for backwards compatibility, not used in S3 approach)
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./agricapture.db")
    
    # Security
    SECRET_KEY = os.getenv("SECRET_KEY", "change-this-secret-key-in-production")


settings = Settings()
