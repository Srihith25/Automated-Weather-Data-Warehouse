CREATE TABLE dim_location (
location_id SERIAL PRIMARY KEY,
city_name VARCHAR(100) NOT NULL,
country VARCHAR(100) NOT NULL,
latitude DECIMAL(9,6),
longitude DECIMAL(9,6),
timezone VARCHAR(50),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
UNIQUE(city_name, country)
);

INSERT INTO dim_location(city_name, country, latitude, longitude, timezone) VALUES
('Bellevue', 'USA', 47.6101, -122.2015, 'America/Los Angeles'),
('Ames', 'USA', 42.0308, -93.6319, 'America/Chicago'),
('Hyderabad', 'India', 17.3850, 78.4867, 'Asia/Kolkata'),
('Berlin', 'Germany', 52.5200, 13.4050, 'Europe/Berlin'),
('Sydney', 'Australia', -33.8688, 151.2093, 'Australia/Sydney'),
('Delhi', 'India', 28.7041, 77.1025, 'Asia/Kolkata'),
('Cape Town', 'South Africa', -33.9249, 18.4241, 'Africa/Johannesburg'),
('Rio de Janeiro', 'Brazil', -22.9068, -43.1729, 'America/Sao_Paulo'),
('London', 'UK', 51.5074, -0.1278, 'Europe/London'),
('Jakarta', 'Indonesia', -6.2088, 106.8456, 'Asia/Jakarta');

CREATE TABLE dim_date(
date_id SERIAL PRIMARY KEY,
full_date DATE NOT NULL UNIQUE,
year INT NOT NULL,
quarter INT NOT NULL,
month INT NOT NULL,
month_name VARCHAR(20) NOT NULL,
week_of_year INT NOT NULL,
day_of_month INT NOT NULL,
day_of_week INT NOT NULL,
day_name VARCHAR(20) NOT NULL,
is_weekend BOOLEAN NOT NULL
);

CREATE TABLE dim_time(
time_id SERIAL PRIMARY KEY,
hour INT NOT NULL,
minute INT NOT NULL,
time_of_day VARCHAR(20) NOT NULL,
hour_24 VARCHAR(5) NOT NULL,
UNIQUE(hour, minute)
);

INSERT INTO dim_time(hour, minute,time_of_day, hour_24)
SELECT
hour,
0 as minute,
CASE
WHEN hour <= 6 AND hour < 12 THEN 'Morning'
WHEN hour >=12 AND hour <18 THEN 'Afternoon'
WHEN hour >=18 AND hour <22 THEN 'Evening'
ELSE 'Night'

END as time_of_day,
LPAD(hour::TEXT, 2, '0') || ':0' as hour_24

FROM generate_series(0,23) as hour;

CREATE TABLE dim_weather_condition(
condition_id SERIAL PRIMARY KEY,
main_condition VARCHAR(50) NOT NULL UNIQUE,
description VARCHAR(200)
);

INSERT INTO dim_Weather_condition (main_condition,description) VALUES
('Clear', 'Clear Sky'),
('Clouds', 'Cloudy Conditions'),
('Rain', 'Rainy weather'),
('Drizzle', 'Light rain'),
('Thunderstorm', 'Thunderstorm conditions'),
('Snow', 'Snowy weather'),
('Mist', 'Misty conditions'),
('Fog', 'Foggy weather'),
('Haze', 'Hazy conditions'),
('Dust', 'Dusty conditions'),
('Smoke', 'Smoky conditions');

select * from dim_location;
ALTER TABLE dim_date
ADD COLUMN month INTEGER;
ALTER TABLE dim_date
DROP COLUMN mont;