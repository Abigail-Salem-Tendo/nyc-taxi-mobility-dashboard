# this python script maps the database tables to Python classes
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class ZoneLookup(db.Model):
    __tablename__ = 'zone_lookup'
    location_id = db.Column(db.Integer, primary_key=True)
    borough = db.Column(db.String(50))
    zone_name = db.Column(db.String(50))
    service_zone = db.Column(db.String(50))

    def to_dict(self):
        #Converto to dictionary for JSON responses
        return {
            'location_id': self.location_id,
            'borough': self.borough,
            'zone_name': self.zone_name,
            'service_zone': self.service_zone
        }

class ZoneGeo(db.Model):
    __tablename__ = 'zone_geo'
    location_id = db.Column(db.Integer, primary_key=True)
    zone_geojson = db.Column(db.Text, nullable=False)

    def to_dict(self):
        import json
        return {
            'location_id': self.location_id,
            'geometry': json.loads(self.zone_geojson)
        }

class Tripdata(db.Model):
    __tablename__ = 'tripdata'
    trip_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    vendor_id = db.Column(db.SmallInteger)
    pickup_datetime = db.Column(db.DateTime, nullable=False)
    dropoff_datetime = db.Column(db.DateTime, nullable=False)
    passenger_count = db.Column(db.SmallInteger, nullable=False)
    trip_distance = db.Column(db.Numeric(10, 2), nullable=False)
    pulocation_id = db.Column(db.Integer, nullable=False)
    dolocation_id = db.Column(db.Integer, nullable=False)
    fare_amount = db.Column(db.Numeric(10, 2), nullable=False)
    tip_amount = db.Column(db.Numeric(10, 2), default=0)
    total_amount = db.Column(db.Numeric(10, 2), nullable=False)
    avg_speed_mph = db.Column(db.Numeric(10, 2))
    congestion_level = db.Column(db.String(10))
    fare_per_mile = db.Column(db.Numeric(10, 2))
    trip_duration_min = db.Column(db.Integer)
    hour_of_day = db.Column(db.SmallInteger)
    day_of_week = db.Column(db.String(50))
    is_peak_hour = db.Column(db.Boolean)

    def to_dict(self):
        return {
            'trip_id': self.trip_id,
            'pickup_datetime': self.pickup_datetime.isoformat() if self.pickup_datetime else None,
            'dropoff_datetime': self.dropoff_datetime.isoformat() if self.dropoff_datetime else None,
            'trip_distance': float(self.trip_distance) if self.trip_distance else None,
            'fare_amount': float(self.fare_amount) if self.fare_amount else None,
            'avg_speed_mph': float(self.avg_speed_mph) if self.avg_speed_mph else None,
            'congestion_level': self.congestion_level,
            'hour_of_day': self.hour_of_day,
            'day_of_week': self.day_of_week,
            'is_peak_hour': self.is_peak_hour
        }

class ExcludedData(db.Model):
    __tablename__ = 'excluded_data_log'

    log_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    issue_type = db.Column(db.String(50), nullable=False)
    trip_identifier = db.Column(db.String(255))
    field_name = db.Column(db.String(50))
    issue_description = db.Column(db.Text)
    action_taken = db.Column(db.String(100))

    def to_dict(self):
        return {
            'log_id': self.log_id,
            'log_timestamp': self.log_timestamp.isoformat() if self.log_timestamp else None,
            'issue_type': self.issue_type,
            'issue_description': self.issue_description
        }