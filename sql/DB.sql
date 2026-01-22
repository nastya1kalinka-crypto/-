--Таблица станций
CREATE TABLE IF NOT EXISTS weather_stations (
    station_id SERIAL PRIMARY KEY,
    station_name VARCHAR(100) NOT NULL,
    location VARCHAR(200),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    altitude DECIMAL(6,2),
    installed_date DATE DEFAULT CURRENT_DATE
);

--Основная таблица измерений
CREATE TABLE IF NOT EXISTS weather_data (
    measurement_id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES weather_stations(station_id),
    temperature DECIMAL(4,1),    -- °C
    humidity DECIMAL(4,1),       -- %
    pressure DECIMAL(6,1),       -- мм рт.ст.
    wind_speed DECIMAL(4,1),     -- м/с
    wind_direction VARCHAR(3),   -- направление ветра (N, NE, E, SE, S, SW, W, NW)
    precipitation DECIMAL(5,1)   
);

CREATE TABLE IF NOT EXISTS air_quality (
    measurement_id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES weather_stations(station_id),
    pm25 DECIMAL(4,1),      -- мелкие частицы (μg/m³)
    pm10 DECIMAL(4,1),      -- крупные частицы (μg/m³)
    no2 DECIMAL(4,1),       -- диоксид азота (ppb)
    so2 DECIMAL(4,1),       -- диоксид серы (ppb)
    o3 DECIMAL(4,1),        -- озон (ppb)
    co DECIMAL(4,1),        -- угарный газ (ppm)
    aqi INTEGER,            -- общий индекс качества воздуха
    health_impact VARCHAR(50),
    source_type VARCHAR(50) -- источник загрязнения
);

CREATE INDEX IF NOT EXISTS idx_air_quality_station ON air_quality(station_id);
CREATE INDEX IF NOT EXISTS idx_weather_data_station ON weather_data(station_id)