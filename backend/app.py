from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, func, text
from dotenv import load_dotenv
import os
from datetime import datetime
from models import db, ZoneLookup, ZoneGeo, Tripdata, ExcludedData

load_dotenv()

app = Flask(__name__)
CORS(app)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# Database connection checker endpoint
@app.route('/api/db_connection', methods=['GET'])
def check_db():
    try:
        db.session.execute(db.text('SELECT 1'))
        return jsonify({'status': 'success', 'message': f'{os.getenv("DB_NAME")} database connected successfully'})
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'{os.getenv("DB_NAME")} database connection failed'}), 500


# Statistics of all average stored data in the database
@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    try:
        results = db.session.query(
            func.count(Tripdata.trip_id).label('total_trips'),
            func.round(func.avg(Tripdata.avg_speed_mph), 2).label('avg_speed_mph'),
            func.round(func.avg(Tripdata.fare_per_mile), 2).label('avg_fare_per_mile'),
            func.round(func.avg(Tripdata.trip_duration_min), 1).label('avg_duration_min'),
            func.round(func.sum(Tripdata.fare_amount), 2).label('total_revenue'),
            func.round(func.avg(Tripdata.trip_distance), 2).label('avg_distance_miles'),
            func.round(func.avg(Tripdata.passenger_count), 2).label('avg_passengers'),
            func.round(func.avg(Tripdata.tip_amount), 2).label('avg_tip')
        ).first()

        return jsonify({
            'total_trips': results.total_trips,
            'average_speed_in_milesperhour': float(results.avg_speed_mph) if results.avg_speed_mph else 0,
            'average_fare_per_mile': float(results.avg_fare_per_mile) if results.avg_fare_per_mile else 0,
            'average_duration_min': float(results.avg_duration_min) if results.avg_duration_min else 0,
            'total_fare_amount': float(results.total_revenue) if results.total_revenue else 0,
            'average_distance_miles': float(results.avg_distance_miles) if results.avg_distance_miles else 0,
            'average_passengers': float(results.avg_passengers) if results.avg_passengers else 0,
            'average_tip_amount': float(results.avg_tip) if results.avg_tip else 0
    })
    
    except Exception as e:
        return jsonify({'status': 'Failure', 'message': f'Error retrieving statistics: {str(e)}'}), 500
    

if __name__ == '__main__':
    app.run(debug=True, port=5000)