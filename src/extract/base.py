from abc import ABC, abstractmethod
from typing import Dict, List, Any
from datetime import datetime

class WeatherAPIBase(ABC):
    """Base class for weather API extractors."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        
    @abstractmethod
    def get_current_weather(self, city: str, country: str) -> Dict[str, Any]:
        """Get current weather for a city."""
        pass
    
    @abstractmethod
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parse API response to standard format."""
        pass