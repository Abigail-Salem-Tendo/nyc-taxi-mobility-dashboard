-- Set Up the database
CREATE DATABASE IF NOT EXISTS mobility_db;
USE mobility_db;

-- Table 1: Reference for Zone Names
CREATE TABLE dim_zones (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50)
);

-- Table 2: Reference for Spatial Shapes
CREATE TABLE zone_geometry (
    location_id INT PRIMARY KEY,
    zone_geojson LONGTEXT NOT NULL,
    CONSTRAINT fk_zone_geo FOREIGN KEY (location_id)
        REFERENCES dim_zones(location_id)
);

-- Table 3: reference for yellow_tripdata.csv file
CREATE TABLE fact_trips (
    trip_id INT AUTO_INCREMENT PRIMARY KEY,
    vendor_id TINYINT,
    pickup_datetime DATETIME NOT NULL,
    dropoff_datetime DATETIME NOT NULL,
    passenger_count TINYINT NOT NULL,
    trip_distance DECIMAL(10, 2) NOT NULL,
    pulocation_id INT NOT NULL,
    dolocation_id INT NOT NULL,
    fare_amount DECIMAL(10, 2) NOT NULL,
    tip_amount DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    -- Derived Columns for Insights
    avg_speed_mph DECIMAL(10, 2),
    congestion_level VARCHAR(10),

    -- values that will be auto generated in mysql
    trip_duration_min INT GENERATED ALWAYS AS (TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime)) STORED,
    hour_of_day TINYINT GENERATED ALWAYS AS (HOUR(pickup_datetime)) STORED,
    day_of_week TINYINT GENERATED ALWAYS AS (DAYOFWEEK(pickup_datetime)) STORED,
    is_peak_hour BOOLEAN GENERATED ALWAYS AS (HOUR(pickup_datetime) IN (7, 8, 9, 17, 18, 19)) STORED,

    FOREIGN KEY (pulocation_id) REFERENCES dim_zones(location_id),
    FOREIGN KEY (dolocation_id) REFERENCES dim_zones(location_id)
) ENGINE=InnoDB;

-- Add Performance Indexes to help with dashboard lookup
CREATE INDEX idx_pickup_time ON fact_trips(pickup_datetime);
CREATE INDEX idx_hour_of_day ON fact_trips(hour_of_day);
CREATE INDEX idx_peak_hour ON fact_trips(is_peak_hour);
CREATE INDEX idx_pulocation ON fact_trips(pulocation_id);
CREATE INDEX idx_dolocation ON fact_trips(dolocation_id);
CREATE INDEX idx_od_pair ON fact_trips(pulocation_id, dolocation_id);
CREATE INDEX idx_congestion_level ON fact_trips(congestion_level);