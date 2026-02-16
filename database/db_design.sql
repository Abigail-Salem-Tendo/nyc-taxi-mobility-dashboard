-- Set Up the database
CREATE DATABASE IF NOT EXISTS nyc_mobility_data;
USE nyc_mobility_data;

-- Table to store values from taxi zones csv file
CREATE TABLE zone_lookup
(
    location_id  INT PRIMARY KEY,
    borough      VARCHAR(50),
    zone_name    VARCHAR(100),
    service_zone VARCHAR(50),

    INDEX idx_borough (borough)
);

-- Table to reference the taxi_zones shape file
CREATE TABLE zone_geo (
    location_id INT PRIMARY KEY,
    zone_geojson LONGTEXT NOT NULL,
    FOREIGN KEY (location_id)
        REFERENCES zone_lookup(location_id)
);

-- Trips table for yellow_tripdata.csv file
CREATE TABLE trip_data (
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

    FOREIGN KEY (pulocation_id) REFERENCES zone_lookup(location_id),
    FOREIGN KEY (dolocation_id) REFERENCES zone_lookup(location_id),

    CHECK (trip_distance >= 0),
    CHECK (fare_amount >= 0),
    CHECK (pickup_datetime < dropoff_datetime)
);

-- Add Performance Indexes to help with dashboard lookup
CREATE INDEX idx_pickup_time ON trip_data(pickup_datetime);
CREATE INDEX idx_hour_of_day ON trip_data(hour_of_day);
CREATE INDEX idx_peak_hour ON trip_data(is_peak_hour);
CREATE INDEX idx_pulocation ON trip_data(pulocation_id);
CREATE INDEX idx_dolocation ON trip_data(dolocation_id);
CREATE INDEX idx_od_pair ON trip_data(pulocation_id, dolocation_id);
CREATE INDEX idx_congestion_level ON trip_data(congestion_level);

-- a table to show the excluded data logs
CREATE TABLE excluded_data_log
(
    log_id            INT AUTO_INCREMENT PRIMARY KEY,
    issue_type        VARCHAR(50) NOT NULL,
    trip_identifier   VARCHAR(255),
    field_name        VARCHAR(50),
    issue_description TEXT,
    action_taken      VARCHAR(100),

    INDEX idx_issue_type (issue_type)
);