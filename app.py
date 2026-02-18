from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

app = Flask(__name__)
CORS(app)

# Database configuration
def get_engine():
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME')

    url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(url)

engine = get_engine()

def query_db(query, params=None):
    with engine.connect() as connection:
        result = connection.execute(text(query), params or {})
        return [dict(row) for row in result.mappings().all()]
    
# Database connection check endpoint
@app.route('/api/db_connection', methods=['GET'])
def db_connection():
    try:
        query_db('SELECT 1')
        return jsonify({
            'status': 'ok',
            'message': f'{os.getenv("DB_NAME")} database connection is successful',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'{os.getenv("DB_NAME")} database connection has failed: {str(e)}',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'error': str(e)
        }), 500

# Average statistics of all features derived from the data
@app.route('/api/statistics', methods=['GET'])
def statistics():
    with engine.connect() as conn:
        row = conn.execute(text("""
            select
                COUNT(*)                          AS total_trips,
                ROUND(AVG(avg_speed_mph), 2)      AS average_speed_mph,
                ROUND(AVG(fare_per_mile), 2)      AS average_fare_per_mile,
                ROUND(AVG(trip_duration_min), 1)  AS average_duration_min,
                ROUND(SUM(fare_amount), 2)        AS total_fare_amount,
                ROUND(AVG(trip_distance), 2)      AS average_distance_miles,
                ROUND(AVG(passenger_count), 2)    AS average_passengers,
                ROUND(AVG(tip_amount), 2)         AS average_tip
            from trip_data
        """)).fetchone()

    return jsonify(dict(row._mapping))

@app.route('/api/trips-by-hour')
def trips_by_hour():
    rows = query_db("""
        SELECT
            hour_of_day,
            is_peak_hour,
            COUNT(*)                          AS trips_in_the_hour,
            ROUND(AVG(avg_speed_mph), 2)      AS average_speed_in_miles_per_hour,
            ROUND(AVG(fare_per_mile), 2)      AS average_fare_per_mile,
            ROUND(AVG(trip_duration_min), 1)  AS average_duration_in_minutes,
            ROUND(AVG(fare_amount), 2)        AS average_fare_generated,
            ROUND(AVG(passenger_count), 2)    AS average_passengers,
            ROUND(AVG(tip_amount), 2)         AS average_tip_amount
        FROM trip_data
        GROUP BY hour_of_day, is_peak_hour
        ORDER BY hour_of_day
    """)

    for row in rows:
        row['is_peak_hour'] = bool(row['is_peak_hour'])

    return jsonify(rows)


@app.route('/api/trips-by-day')
def trips_by_day():
    rows = query_db("""
        SELECT
            day_of_week,
            COUNT(*)                          AS trips_done_on_the_day,
            ROUND(AVG(avg_speed_mph), 2)      AS average_speed_in_miles_per_hour,
            ROUND(AVG(fare_per_mile), 2)      AS average_fare_per_mile,
            ROUND(AVG(trip_duration_min), 1)  AS average_duration_in_minutes,
            ROUND(AVG(fare_amount), 2)        AS average_fare_generated,
            ROUND(AVG(tip_amount), 2)         AS average_tip_amount
        FROM trip_data
        GROUP BY day_of_week
        ORDER BY field(day_of_week, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
    """)

    return jsonify(rows)


if __name__ == '__main__':
    app.run(debug=True, port=5000)