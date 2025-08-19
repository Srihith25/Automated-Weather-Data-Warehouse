-- Create a view for easy weather data access
CREATE VIEW v_weather_data AS
SELECT 
    l.city_name,
    l.country,
    d.full_date,
    t.hour_24 as time,
    t.time_of_day,
    wc.main_condition as weather_condition,
    f.temperature_celsius,
    f.temperature_fahrenheit,
    f.humidity_percent,
    f.pressure_hpa,
    f.wind_speed_mps,
    f.visibility_meters,
    f.recorded_at
FROM fact_weather_measurements f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_time t ON f.time_id = t.time_id
LEFT JOIN dim_weather_condition wc ON f.condition_id = wc.condition_id;

-- Create materialized view for daily averages
CREATE MATERIALIZED VIEW mv_daily_weather_summary AS
SELECT 
    l.city_name,
    d.full_date,
    AVG(f.temperature_celsius) as avg_temp_celsius,
    MIN(f.temperature_celsius) as min_temp_celsius,
    MAX(f.temperature_celsius) as max_temp_celsius,
    AVG(f.humidity_percent) as avg_humidity,
    AVG(f.wind_speed_mps) as avg_wind_speed,
    COUNT(*) as measurement_count
FROM fact_weather_measurements f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY l.city_name, d.full_date;