import os
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import PostgresDsn

# Load environment variables
load_dotenv()

class Settings(BaseSettings):
    """Application settings."""
    
    # Database
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", 5432))
    db_name: str = os.getenv("DB_NAME", "weather")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "")
    
    # API Keys
    openweather_api_key: str = os.getenv("OPENWEATHER_API_KEY", "")
    noaa_api_token: str = os.getenv("NOAA_API_TOKEN", "")
    weatherapi_key: str = os.getenv("WEATHERAPI_KEY", "")
    
    # Application
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    batch_size: int = int(os.getenv("BATCH_SIZE", 1000))
    
    # Cities to monitor
    cities: List[Dict[str, Any]] = [
        {"name": "Bellevue", "country": "USA", "state": "WA"},
        {"name": "Ames", "country": "USA", "state": "IA"},
        {"name": "Hyderabad", "country": "India", "state": "TG"},
        {"name": "Berlin", "country": "Germany", "state": ""},
        {"name": "Sydney", "country": "Australia", "state": "NSW"},
        {"name": "Delhi", "country": "India", "state": "DL"},
        {"name": "Cape Town", "country": "South Africa", "state": ""},
        {"name": "Rio de Janeiro", "country": "Brazil", "state": "RJ"},
        {"name": "London", "country": "UK", "state": ""},
        {"name": "Jakarta", "country": "Indonesia", "state": ""}
    ]
    
    @property
    def database_url(self) -> str:
        """Get database URL."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()