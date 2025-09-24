
-- PostgreSQL initialization script for IMARIKA Weather Data Pipeline
-- Creates necessary tables for raw and clean weather data

-- Create raw weather data table
CREATE TABLE IF NOT EXISTS weather_raw (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE weather_clean (
    reading_id               VARCHAR(50) PRIMARY KEY,
    device_id                VARCHAR(50) NOT NULL,
    valid                    BOOLEAN,
    uv_index                 DOUBLE PRECISION,
    rain_gauge               DOUBLE PRECISION,
    wind_speed               DOUBLE PRECISION,
    air_humidity             INTEGER,
    peak_wind_gust           DOUBLE PRECISION,
    air_temperature          DOUBLE PRECISION,
    light_intensity          INTEGER,
    rain_accumulation        DOUBLE PRECISION,
    barometric_pressure      DOUBLE PRECISION,
    wind_direction_sensor    INTEGER,
    processing_timestamp     TIMESTAMP,
);


-- Create clean weather data table
CREATE TABLE IF NOT EXISTS daily_agg (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    date_epoch INTEGER NOT NULL,
    maxtemp_c DOUBLE PRECISION,
    mintemp_c DOUBLE PRECISION,
    avgtemp_c DOUBLE PRECISION,
    maxwind_kph DOUBLE PRECISION,
    totalprecip_mm DOUBLE PRECISION,
    avghumidity INTEGER,
    daily_will_it_rain INTEGER,
    daily_chance_of_rain INTEGER,
    processing_timestamp TIMESTAMP,
    anomaly_score DOUBLE PRECISION,
    is_anomaly BOOLEAN
);


-- Optional test connection table (for JDBC or system health checks)
CREATE TABLE IF NOT EXISTS test_connection (
    id SERIAL PRIMARY KEY,
    status TEXT DEFAULT 'ok'
);

-- QC Audit Log table
CREATE TABLE qc1_audit_log (
    reading_id               VARCHAR(50) PRIMARY KEY,
    device_id                VARCHAR(50),
    uv_index                 DOUBLE PRECISION,
    rain_gauge               DOUBLE PRECISION,
    wind_speed               DOUBLE PRECISION,
    air_humidity             DOUBLE PRECISION,
    peak_wind_gust           DOUBLE PRECISION,
    air_temperature          DOUBLE PRECISION,
    light_intensity          DOUBLE PRECISION,
    rain_accumulation        DOUBLE PRECISION,
    barometric_pressure      DOUBLE PRECISION,
    wind_direction_sensor    DOUBLE PRECISION,
    time_diff                DOUBLE PRECISION,
    pressure_hpa             DOUBLE PRECISION,
    QC_time_gap              VARCHAR(20),
    QC_Tair_range            VARCHAR(20),
    QC_RH_range              VARCHAR(20),
    QC_WindSpeed_range       VARCHAR(20),
    QC_WindDir_range         VARCHAR(20),
    QC_WindDir_requires_wind VARCHAR(20),
    QC_Rain_15min            VARCHAR(20),
    tair_step                DOUBLE PRECISION,
    QC_Tair_step             VARCHAR(20),
    daily_valid_frac         DOUBLE PRECISION,
    daily_rain_mm            DOUBLE PRECISION,
    QC_Daily_Availability    VARCHAR(20),
    QC_Rain_Daily            VARCHAR(20),
    processing_timestamp     TIMESTAMP,
    date                     DATE,
);


INSERT INTO test_connection (status) VALUES ('ready') ON CONFLICT DO NOTHING;


-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_weather_raw_received_at ON weather_raw(received_at);
CREATE INDEX IF NOT EXISTS idx_daily_agg_device_id ON daily_agg(device_id);
CREATE INDEX IF NOT EXISTS idx_daily_agg_date ON daily_agg(date);
CREATE INDEX IF NOT EXISTS idx_daily_agg_is_anomaly ON daily_agg(is_anomaly);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
