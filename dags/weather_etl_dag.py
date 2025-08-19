from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Default arguments for the DAG
default_args = {
    'owner': 'weather_etl',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='Extract weather data every hour',
    schedule_interval='0 * * * *',  # Run every hour at minute 0
    catchup=False,
    tags=['weather', 'etl'],
)

def extract_weather_data(**context):
    """Extract weather data from Open-Meteo API."""
    from src.extract.open_meteo import OpenMeteoExtractor
    from src.utils.logger import logger
    
    logger.info("Starting weather data extraction")
    extractor = OpenMeteoExtractor()
    weather_data = extractor.extract_all_cities()
    extractor.close()
    
    # Store in XCom for the next task
    context['task_instance'].xcom_push(key='weather_data', value=weather_data)
    logger.info(f"Extracted data for {len(weather_data)} cities")
    return len(weather_data)

def load_weather_data(**context):
    """Load weather data to PostgreSQL."""
    from src.utils.database import db
    from src.utils.logger import logger
    from datetime import datetime
    
    # Get data from previous task
    weather_data = context['task_instance'].xcom_pull(key='weather_data')
    
    if not weather_data:
        logger.error("No data to load")
        return 0
    
    logger.info(f"Loading {len(weather_data)} records to database")
    
    # Initialize database
    db.initialize()
    
    loaded_count = 0
    
    # Helper functions
    def get_location_id(city, country):
        query = """
            SELECT location_id 
            FROM dim_location 
            WHERE city_name = :city AND country = :country
        """
        result = db.execute_query(query, {"city": city, "country": country})
        row = result.fetchone()
        return row[0] if row else None
    
    def get_or_create_date_id(date):
        query = "SELECT date_id FROM dim_date WHERE full_date = :date"
        result = db.execute_query(query, {"date": date.date()})
        row = result.fetchone()
        
        if row:
            return row[0]
        
        # Insert if not exists
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
        
        result = db.execute_query(insert_query, params)
        return result.fetchone()[0]
    
    def get_time_id(date):
        query = "SELECT time_id FROM dim_time WHERE hour = :hour AND minute = :minute"
        result = db.execute_query(query, {"hour": date.hour, "minute": 0})
        row = result.fetchone()
        return row[0] if row else None
    
    # Load each record
    for data in weather_data:
        try:
            location_id = get_location_id(data["city"], data["country"])
            if not location_id:
                logger.warning(f"Location not found: {data['city']}")
                continue
            
            date_id = get_or_create_date_id(data["recorded_at"])
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
            
        except Exception as e:
            logger.error(f"Error loading data for {data['city']}: {e}")
    
    logger.success(f"Loaded {loaded_count} records")
    return loaded_count

# Define tasks
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    dag=dag,
)

# Set task dependencies
extract_task >> load_task