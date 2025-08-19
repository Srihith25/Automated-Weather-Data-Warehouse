import sys
from pathlib import Path
from loguru import logger
from src.config.config import settings

# Remove default logger
logger.remove()

# Console logger
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level=settings.log_level,
    colorize=True
)

# File logger
log_path = Path("logs")
log_path.mkdir(exist_ok=True)

logger.add(
    log_path / "weather_etl_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level=settings.log_level,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
)

# Export logger
__all__ = ["logger"]