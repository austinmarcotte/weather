CREATE DATABASE amarcotte_weather;

CREATE TABLE staging_forecasts (staging_id SERIAL PRIMARY KEY, source CHARACTER VARYING(16), latitude NUMERIC(7,4), longitude NUMERIC(7,4), weather_date TIMESTAMP, temperature NUMERIC(5,2), wind_speed NUMERIC(5,2), wind_direction NUMERIC(5,2), rain_chance NUMERIC(5,2), timestamp TIMESTAMP DEFAULT NOW());

CREATE TABLE forecasts (forecast_id SERIAL PRIMARY KEY, latitude NUMERIC(7,4), longitude NUMERIC(7,4), forecast_date TIMESTAMP, sources INTEGER, temperature NUMERIC(5,2), wind_speed NUMERIC(5,2), wind_direction NUMERIC(5,2), rain_chance NUMERIC(5,2), timestamp TIMESTAMP DEFAULT NOW());
