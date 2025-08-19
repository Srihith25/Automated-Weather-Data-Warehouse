"""Simple ETL runner for weather data."""
import sys
from pathlib import Path

# Add project root to Python path so we can import from config and src
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime
from extract.open_meteo import OpenMeteoExtractor
from utils.logger import logger
from utils.database import db
from src.config.config import settings

def get_location_id(city: str, country: str) -> int:
    query = """
        SELECT location_id 
        FROM dim_location 
        WHERE city_name = :city AND country = :country
    """
    rows = db.execute_query(query, {"city": city, "country": country})
    row = rows[0] if rows else None
    return row[0] if row else None


def get_date_id(date: datetime) -> int:
    query = """
        SELECT date_id 
        FROM dim_date 
        WHERE full_date = :date
    """
    rows = db.execute_query(query, {"date": date.date()})
    row = rows[0] if rows else None
    
    if row:
        return row[0]
    
    # If not exists, insert it
    insert_query = """
        INSERT INTO dim_date (full_date, year, quarter, month, month_name,
                            week_of_year, day_of_month, day_of_week, day_name, is_weekend)
        VALUES (:full_date, :year, :quarter, :month, :month_name,
                :week_of_year, :day_of_month, :day_of_week, :day_name, :is_weekend)
        RETURNING date_id
    """
    params = {
        "full_date": date.date(),
        "year": date.year,
        "quarter": (date.month - 1) // 3 + 1,
        "month": date.month,
        "month_name": date.strftime("%B"),
        "week_of_year": date.isocalendar()[1],
        "day_of_month": date.day,
        "day_of_week": date.weekday(),
        "day_name": date.strftime("%A"),
        "is_weekend": date.weekday() >= 5
    }
    
    rows = db.execute_query(insert_query, params)
    return rows[0][0]


def get_time_id(date: datetime) -> int:
    query = """
        SELECT time_id 
        FROM dim_time 
        WHERE hour = :hour AND minute = :minute
    """
    rows = db.execute_query(query, {"hour": date.hour, "minute": 0})
    row = rows[0] if rows else None
    return row[0] if row else None

def run_etl():
    """Run the ETL process."""
    logger.info("Starting weather ETL process")
    
    # Initialize database
    db.initialize()
    
    # Extract
    logger.info("Starting extraction phase")
    extractor = OpenMeteoExtractor()
    weather_data = extractor.extract_all_cities()
    extractor.close()
    
    if not weather_data:
        logger.error("No data extracted")
        return
    
    # Load (we're skipping transform since we only need temperature)
    logger.info("Starting load phase")
    loaded_count = 0
    
    for data in weather_data:
        try:
            # Get dimension IDs
            location_id = get_location_id(data["city"], data["country"])
            if not location_id:
                logger.warning(f"Location not found: {data['city']}, {data['country']}")
                continue
            
            date_id = get_date_id(data["recorded_at"])
            time_id = get_time_id(data["recorded_at"])
            
            if not time_id:
                logger.warning(f"Time not found for hour: {data['recorded_at'].hour}")
                continue
            
            # Insert into fact table
            insert_query = """
                INSERT INTO fact_weather_measurements 
                (location_id, date_id, time_id, temperature_celsius, temperature_fahrenheit,
                 api_source, recorded_at)
                VALUES (:location_id, :date_id, :time_id, :temp_c, :temp_f,
                        :api_source, :recorded_at)
            """
            
            params = {
                "location_id": location_id,
                "date_id": date_id,
                "time_id": time_id,
                "temp_c": data["temperature_celsius"],
                "temp_f": data["temperature_fahrenheit"],
                "api_source": data["api_source"],
                "recorded_at": data["recorded_at"]
            }
            
            db.execute_query(insert_query, params)
            loaded_count += 1
            logger.info(f"Loaded data for {data['city']}")
            
        except Exception as e:
            logger.error(f"Error loading data for {data['city']}: {e}")
    
    logger.success(f"ETL complete. Loaded {loaded_count} records")

if __name__ == "__main__":
    run_etl()