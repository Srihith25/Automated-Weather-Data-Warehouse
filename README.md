Automated ETL pipeline that extracts hourly temperature data from 10 global cities, transforms it, and loads into a PostgreSQL data warehouse with star schema. Built with Python, Apache Airflow, and Docker.

Overview:

WeatherFlow is a comprehensive data engineering project that demonstrates modern ETL practices by:
- **Extracting** real-time temperature data from the Open-Meteo API
- **Transforming** raw weather data into analytics-ready formats
- **Loading** processed data into a PostgreSQL data warehouse using star schema design

Technical Stack

- **Language**: Python 3.11
- **Database**: PostgreSQL with Star Schema (Fact & Dimension tables)
- **Orchestration**: Apache Airflow 2.8 (Dockerized)
- **Data Processing**: Pandas, SQLAlchemy
- **API**: Open-Meteo (no API key required)
- **Containerization**: Docker & Docker Compose
- **Logging**: Loguru with structured logging

Key Features

- **Hourly Automation**: Scheduled data collection every hour via Airflow DAGs
- **Star Schema Design**: Optimized fact and dimension tables for analytical queries
- **Error Handling**: Comprehensive logging and retry mechanisms
- **Scalable Architecture**: Docker-based deployment for easy scaling
- **Data Quality**: Built-in validation and monitoring
- **Zero API Costs**: Uses Open-Meteo's free tier with no authentication required

## 🚀 Quick Start


# Clone the repository
git clone https://github.com/yourusername/weatherflow-etl.git
cd weatherflow-etl

# Set up environment
python -m venv weather_etl_env
source weather_etl_env/bin/activate  # On Windows: weather_etl_env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure database
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# Run ETL manually
python src/run_etl.py

# Or start Airflow (Docker required)
docker-compose -f docker-compose-airflow.yml up -d
