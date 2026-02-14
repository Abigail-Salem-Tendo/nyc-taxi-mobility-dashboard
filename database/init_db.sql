-- Set Up the database
CREATE DATABASE IF NOT EXISTS mobility_db;
USE mobility_db;

-- Table to store values from taxi zones csv file
CREATE TABLE dim_zones (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50),

    INDEX idx_borough (borough)
) ENGINE=InnoDB;

-- Table to reference the taxi_zones shape file
CREATE TABLE zone_geometry (
    location_id INT PRIMARY KEY,
    zone_geojson LONGTEXT NOT NULL,
    CONSTRAINT fk_zone_geo FOREIGN KEY (location_id)
        REFERENCES dim_zones(location_id)
) ENGINE=InnoDB;

-- Trips table for yellow_tripdata.csv file
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
    tip_amount DECIMAL(10, 2) DEFAULT 0,
    total_amount DECIMAL(10, 2) NOT NULL,
    -- Derived Columns for Insights
    avg_speed_mph DECIMAL(10, 2),
    congestion_level VARCHAR(10),

    -- values that will be auto generated in mysql
    trip_duration_min INT,
    hour_of_day TINYINT,
    day_of_week TINYINT,
    is_peak_hour BOOLEAN,

    FOREIGN KEY (pulocation_id) REFERENCES dim_zones(location_id),
    FOREIGN KEY (dolocation_id) REFERENCES dim_zones(location_id),

    CHECK (trip_distance >= 0),
    CHECK (fare_amount >= 0),
    CHECK (pickup_datetime < dropoff_datetime)
) ENGINE=InnoDB;

-- Add Performance Indexes to help with dashboard lookup
CREATE INDEX idx_pickup_time ON fact_trips(pickup_datetime);
CREATE INDEX idx_hour_of_day ON fact_trips(hour_of_day);
CREATE INDEX idx_peak_hour ON fact_trips(is_peak_hour);
CREATE INDEX idx_pulocation ON fact_trips(pulocation_id);
CREATE INDEX idx_dolocation ON fact_trips(dolocation_id);
CREATE INDEX idx_od_pair ON fact_trips(pulocation_id, dolocation_id);
CREATE INDEX idx_congestion_level ON fact_trips(congestion_level);

-- a table to show the excluded data logs
CREATE TABLE excluded_data_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    issue_type VARCHAR(50) NOT NULL,
    trip_identifier VARCHAR(255),
    field_name VARCHAR(50),
    issue_description TEXT,
    action_taken VARCHAR(100),

    INDEX idx_issue_type (issue_type)
) ENGINE=InnoDB;