CREATE TABLE fact_weather_measurements (
    measurement_id SERIAL PRIMARY KEY,
    location_id INT NOT NULL,
    date_id INT NOT NULL,
    time_id INT NOT NULL,
    condition_id INT,
    
    -- Weather measurements
    temperature_celsius DECIMAL(5,2),
    temperature_fahrenheit DECIMAL(5,2),
    feels_like_celsius DECIMAL(5,2),
    feels_like_fahrenheit DECIMAL(5,2),
    humidity_percent INT,
    pressure_hpa INT,
    wind_speed_mps DECIMAL(5,2),
    wind_direction_degrees INT,
    wind_gust_mps DECIMAL(5,2),
    visibility_meters INT,
    cloudiness_percent INT,
    precipitation_mm DECIMAL(5,2),
    snow_mm DECIMAL(5,2),
    uv_index DECIMAL(3,1),
    
    -- Metadata
    api_source VARCHAR(50),
    recorded_at TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (condition_id) REFERENCES dim_weather_condition(condition_id)
);

-- Create indexes for better query performance
CREATE INDEX idx_weather_location_date ON fact_weather_measurements(location_id, date_id);
CREATE INDEX idx_weather_recorded_at ON fact_weather_measurements(recorded_at);
CREATE INDEX idx_weather_date_time ON fact_weather_measurements(date_id, time_id);

SELECT * FROM fact_weather_measurements;