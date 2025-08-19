from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from config.config import settings
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Database connection manager."""
    
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        
    def initialize(self):
        """Initialize database connection."""
        try:
            # Create engine
            self.engine = create_engine(
                settings.database_url,
                poolclass=NullPool,  # Don't use connection pooling for ETL
                echo=False  # Set to True for SQL debugging
            )
            
            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info("Database connection successful")
                
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def get_session(self):
        """Get database session."""
        if not self.SessionLocal:
            self.initialize()
        return self.SessionLocal()
    
    def execute_query(self, query, params=None):
        """Execute a raw SQL query."""
        if not self.engine:
            self.initialize()
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            
            # For SELECT queries, return all rows
            if query.strip().lower().startswith("select"):
                return result.fetchall()
            else:
                conn.commit()
                return None

# Global database instance
db = DatabaseConnection()