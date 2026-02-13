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

CREATE TABLE summary_hourly_borough (
    summary_id INT AUTO_INCREMENT PRIMARY KEY,
    pickup_hour TINYINT,
    borough VARCHAR(50),
    total_trips INT,
    avg_fare_amount DECIMAL(10, 2),
    avg_trip_distance DECIMAL(10, 2),

    INDEX (pickup_hour),
    INDEX (borough)
);

CREATE TABLE summary_congestion_stats (
    congestion_level VARCHAR(10),
    borough VARCHAR(50),
    trip_count INT,
    avg_speed DECIMAL(10, 2),

    PRIMARY KEY (congestion_level, borough)
);
-- Table 3: reference for yellow_tripdata.csv file
CREATE TABLE fact_trips (
    trip_id INT AUTO_INCREMENT PRIMARY KEY,
    vendor_id TINYINT,
    pickup_datetime DATETIME NOT NULL,
    dropoff_datetime DATETIME NOT NULL,
    passenger_count TINYINT,
    trip_distance DECIMAL(10, 2) NOT NULL,
    pulocation_id INT NOT NULL,
    dolocation_id INT NOT NULL,
    fare_amount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    -- Derived Columns for Insights
    avg_speed_mph DECIMAL(10, 2),
    congestion_level VARCHAR(10),

    FOREIGN KEY (pulocation_id) REFERENCES dim_zones(location_id),
    FOREIGN KEY (dolocation_id) REFERENCES dim_zones(location_id)
) ENGINE=InnoDB;

-- Add Performance Indexes to help with dashboard lookup
CREATE INDEX idx_pickup_time ON fact_trips(pickup_datetime);
CREATE INDEX idx_pulocation ON fact_trips(pulocation_id);
CREATE INDEX idx_distance_fare ON fact_trips(trip_distance, fare_amount);