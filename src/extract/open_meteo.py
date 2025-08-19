import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
import time
from utils.logger import logger
from config.config import settings

class OpenMeteoExtractor:
    """Extract weather data from Open-Meteo API."""
    
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    
    # City coordinates (since Open-Meteo uses lat/lon)
    CITY_COORDINATES = {
        "Bellevue": {"lat": 47.6101, "lon": -122.2015, "timezone": "America/Los_Angeles"},
        "Ames": {"lat": 42.0308, "lon": -93.6319, "timezone": "America/Chicago"},
        "Hyderabad": {"lat": 17.3850, "lon": 78.4867, "timezone": "Asia/Kolkata"},
        "Berlin": {"lat": 52.5200, "lon": 13.4050, "timezone": "Europe/Berlin"},
        "Sydney": {"lat": -33.8688, "lon": 151.2093, "timezone": "Australia/Sydney"},
        "Delhi": {"lat": 28.7041, "lon": 77.1025, "timezone": "Asia/Kolkata"},
        "Cape Town": {"lat": -33.9249, "lon": 18.4241, "timezone": "Africa/Johannesburg"},
        "Rio de Janeiro": {"lat": -22.9068, "lon": -43.1729, "timezone": "America/Sao_Paulo"},
        "London": {"lat": 51.5074, "lon": -0.1278, "timezone": "Europe/London"},
        "Jakarta": {"lat": -6.2088, "lon": 106.8456, "timezone": "Asia/Jakarta"}
    }
    
    def __init__(self):
        """Initialize the Open-Meteo extractor."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Weather-ETL-Project/1.0'
        })
    
    def get_current_weather(self, city: str) -> Optional[Dict[str, Any]]:
        """
        Get current weather data for a city.
        
        Args:
            city: City name
            
        Returns:
            Weather data dictionary or None if error
        """
        if city not in self.CITY_COORDINATES:
            logger.error(f"City {city} not found in coordinates mapping")
            return None
        
        coords = self.CITY_COORDINATES[city]
        
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "current": "temperature_2m",  # Only temperature as requested
            "temperature_unit": "celsius",
            "timezone": coords["timezone"]
        }
        
        try:
            logger.info(f"Fetching weather data for {city}")
            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse the response
            return self.parse_response(city, data)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {city}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {city}: {e}")
            return None
    
    def parse_response(self, city: str, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Open-Meteo API response.
        
        Args:
            city: City name
            response: API response
            
        Returns:
            Standardized weather data
        """
        current = response.get("current", {})
        
        # Extract timestamp and convert to datetime
        timestamp_str = current.get("time", "")
        try:
            recorded_at = datetime.fromisoformat(timestamp_str)
        except:
            recorded_at = datetime.utcnow()
        
        # Get temperature in Celsius
        temp_celsius = current.get("temperature_2m", None)
        
        # Calculate Fahrenheit
        temp_fahrenheit = (temp_celsius * 9/5) + 32 if temp_celsius is not None else None
        
        return {
            "city": city,
            "country": self._get_country(city),
            "latitude": response.get("latitude"),
            "longitude": response.get("longitude"),
            "temperature_celsius": round(temp_celsius, 2) if temp_celsius else None,
            "temperature_fahrenheit": round(temp_fahrenheit, 2) if temp_fahrenheit else None,
            "recorded_at": recorded_at,
            "api_source": "open_meteo"
        }
    
    def _get_country(self, city: str) -> str:
        """Get country for a city."""
        city_country_map = {
            "Bellevue": "USA",
            "Ames": "USA",
            "Hyderabad": "India",
            "Berlin": "Germany",
            "Sydney": "Australia",
            "Delhi": "India",
            "Cape Town": "South Africa",
            "Rio de Janeiro": "Brazil",
            "London": "UK",
            "Jakarta": "Indonesia"
        }
        return city_country_map.get(city, "Unknown")
    
    def extract_all_cities(self) -> List[Dict[str, Any]]:
        """
        Extract weather data for all cities.
        
        Returns:
            List of weather data dictionaries
        """
        weather_data = []
        
        for city in self.CITY_COORDINATES.keys():
            data = self.get_current_weather(city)
            if data:
                weather_data.append(data)
            
            # Be nice to the API - add a small delay between requests
            time.sleep(0.5)
        
        logger.info(f"Successfully extracted data for {len(weather_data)} cities")
        return weather_data
    
    def close(self):
        """Close the session."""
        self.session.close()