"""Test the Open-Meteo extraction."""
from src.extract.open_meteo import OpenMeteoExtractor
from src.utils.logger import logger
import json
from datetime import datetime

def test_single_city():
    """Test extraction for a single city."""
    extractor = OpenMeteoExtractor()
    
    # Test with Bellevue
    logger.info("Testing single city extraction: Bellevue")
    data = extractor.get_current_weather("Bellevue")
    
    if data:
        logger.success("Successfully extracted data:")
        print(json.dumps(data, indent=2, default=str))
    else:
        logger.error("Failed to extract data")
    
    extractor.close()

def test_all_cities():
    """Test extraction for all cities."""
    extractor = OpenMeteoExtractor()
    
    logger.info("Testing extraction for all cities")
    all_data = extractor.extract_all_cities()
    
    logger.info(f"Extracted data for {len(all_data)} cities:")
    
    # Display results in a table format
    print("\n" + "="*80)
    print(f"{'City':<20} {'Temperature (°C)':<20} {'Temperature (°F)':<20} {'Time':<20}")
    print("="*80)
    
    for data in all_data:
        print(f"{data['city']:<20} {data['temperature_celsius']:<20} "
              f"{data['temperature_fahrenheit']:<20} {data['recorded_at'].strftime('%Y-%m-%d %H:%M'):<20}")
    
    print("="*80)
    
    # Save to JSON file for inspection
    with open('data/weather_extract_sample.json', 'w') as f:
        json.dump(all_data, f, indent=2, default=str)
    
    logger.info("Sample data saved to data/weather_extract_sample.json")
    
    extractor.close()

if __name__ == "__main__":
    print("Open-Meteo Weather Extraction Test")
    print("="*50)
    
    # Test single city
    test_single_city()
    
    print("\n" + "="*50 + "\n")
    
    # Test all cities
    test_all_cities()