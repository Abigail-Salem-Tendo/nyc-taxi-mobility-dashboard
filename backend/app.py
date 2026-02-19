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
    
# Endpoint to get trip statistics by hour of day
@app.route('/api/trips-by-hour')
def trips_by_hour():
    results = db.session.query(
        Tripdata.hour_of_day,
        Tripdata.is_peak_hour,
        func.count(Tripdata.trip_id).label('trip_count'),
        func.round(func.avg(Tripdata.avg_speed_mph), 2).label('avg_speed_mph'),
        func.round(func.avg(Tripdata.fare_per_mile), 2).label('avg_fare_per_mile'),
        func.round(func.avg(Tripdata.trip_duration_min), 1).label('avg_duration_min'),
        func.round(func.avg(Tripdata.fare_amount), 2).label('avg_fare')
    ).group_by(
        Tripdata.hour_of_day,
        Tripdata.is_peak_hour
    ).order_by(
        Tripdata.hour_of_day
    ).all()
    
    return jsonify([{
        'hour_of_day': r.hour_of_day,
        'is_peak_hour': r.is_peak_hour,
        'trip_count': r.trip_count,
        'avg_speed_mph': float(r.avg_speed_mph) if r.avg_speed_mph else 0,
        'avg_fare_per_mile': float(r.avg_fare_per_mile) if r.avg_fare_per_mile else 0,
        'avg_duration_min': float(r.avg_duration_min) if r.avg_duration_min else 0,
        'avg_fare': float(r.avg_fare) if r.avg_fare else 0
    } for r in results])



# function to count zones manually 

def count_zones_manual(zone_data):
    
    zone_counts = {}
    
    for zone_id in zone_data:
        if zone_id in zone_counts:
            zone_counts[zone_id] = zone_counts[zone_id] + 1
        else:
            zone_counts[zone_id] = 1
    
    return zone_counts

# function to sort zones by count using selection sort

def selection_sort_zones(zone_list):
   
    n = len(zone_list)
    
    for i in range(n):
    
        max_index = i
        
        for j in range(i + 1, n):
    
            if zone_list[j][1] > zone_list[max_index][1]:
                max_index = j

        zone_list[i], zone_list[max_index] = zone_list[max_index], zone_list[i]
    
    return zone_list


def get_zone_name_from_db(zone_id):
  
    try:
        zone = ZoneLookup.query.get(zone_id)
        if zone:
            return f"{zone.zone_name}, {zone.borough}"
        return f"Zone {zone_id}"
    except:
        return f"Zone {zone_id}"
    
    
# Algorithm endpoints for top pickup and dropoff zones using manual counting and selection sort

@app.route('/api/top-pickup-zones', methods=['GET'])
def top_pickup_zones():
  
   
    try:
       
        limit = request.args.get('limit', 10, type=int)
        if limit < 1 or limit > 50:
            limit = 10
        
        # Fetch list of zone IDs 
        trips = Tripdata.query.with_entities(Tripdata.pulocation_id).all()
        zone_ids = [trip.pulocation_id for trip in trips]
        total_trips = len(zone_ids)
        
        # Count trip per zone
        zone_counts = count_zones_manual(zone_ids)
        
        # Convert dictionary to list of tuples for sorting
        zone_list = [(zone_id, count) for zone_id, count in zone_counts.items()]
        
        # sort using manual selection sort 
        sorted_zones = selection_sort_zones(zone_list)
        
        # Get top zones based on limit
        top_zones = sorted_zones[:limit]
        
        # format JSON response
        result_data = []
        for rank, (zone_id, count) in enumerate(top_zones, start=1):
            zone_name = get_zone_name_from_db(zone_id)
            percentage = (count / total_trips * 100) if total_trips > 0 else 0
            
            result_data.append({
                'rank': rank,
                'zone_id': int(zone_id),
                'zone_name': zone_name,
                'trip_count': count,
                'percentage': round(percentage, 2)
            })
        
        return jsonify({
            'success': True,
            'data': result_data,
            'metadata': {
                'total_trips': total_trips,
                'unique_zones': len(zone_counts),
                'limit': limit,
                'algorithm': 'Manual Counting (O(n)) + Selection Sort (O(n²))'
            }
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/top-dropoff-zones', methods=['GET'])
def top_dropoff_zones():
   
    try:
        limit = request.args.get('limit', 10, type=int)
        if limit < 1 or limit > 50:
            limit = 10
        
        # get list of dropoff zone ids from database
        trips = Tripdata.query.with_entities(Tripdata.dolocation_id).all()
        
        # extract zone ids from query result
        zone_ids = [trip.dolocation_id for trip in trips]

        #calculate total trips 
        total_trips = len(zone_ids)
        
        # count trips per zone using manual counting algorithm
        zone_counts = count_zones_manual(zone_ids)

        zone_list = [(zone_id, count) for zone_id, count in zone_counts.items()]
        
        sorted_zones = selection_sort_zones(zone_list)
        
        # get top zones based on limit
        top_zones = sorted_zones[:limit]

        # format JSON response
        result_data = []
        for rank, (zone_id, count) in enumerate(top_zones, start=1):
            zone_name = get_zone_name_from_db(zone_id)
            percentage = (count / total_trips * 100) if total_trips > 0 else 0
            
            result_data.append({
                'rank': rank,
                'zone_id': int(zone_id),
                'zone_name': zone_name,
                'trip_count': count,
                'percentage': round(percentage, 2)
            })
        
        return jsonify({
            'success': True,
            'data': result_data,
            'metadata': {
                'total_trips': total_trips,
                'unique_zones': len(zone_counts),
                'limit': limit,
                'algorithm': 'Manual Counting (O(n)) + Selection Sort (O(n²))'
            }
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

    

 # implemented an endpoint to explain algorithms used in the top pickup and dropoff zones endpoints.

@app.route('/api/algorithm-info', methods=['GET'])
def algorithm_info():
  
    return jsonify({
        'success': True,
        'project': 'NYC Taxi Mobility Dashboard',
        'algorithms': [
            {
                'name': 'Manual Counting Algorithm',
                'purpose': 'Count trips per zone',
                'time_complexity': 'O(n) iterates through all trips once',
                'pseudo_code': [
                    '1. Create empty dictionary zone_counts',
                    '2. For each zone_id in dataset:',
                    '  - if we have already seen the zone_id, add 1 to its count',
                    '  - if we have seen it yet , add it to the dictionary and set its count to 1',
                    '3. Return zone_counts dictionary'
                ],
            },
            {
                'name': 'Selection Sort',
                'purpose': 'Sort zones by trip count from highest to lowest',
                'time_complexity': 'O(n²) uses nestd loops to find maximum value for each position',
                'pseudo_code': [
                    '1. look at every position i',
                    '2. assume that i is the max (the biggest)',
                    '3. look at numbers after i ',
                    '4. if you find a bigger number than current max, remember its position',
                    '5. Swap current position i with max index (the biggest number found)',
                    '6. Return sorted list'
                ],
                
    
            }
        ],
        'implementation_details': {
            'no_libraries_used': 'The algorithms were implemented manually without using any built-in sorting or counting functions.'
        },
        'real_world_application': {
            'problem': 'Which taxi zones are busiest?',
            'solution': 'use algorithms to identify high-demand zones so as to optimize taxi dispatch and urban planning',
        },
        'endpoints_using_algorithms': {
            'pickup_zones': '/api/top-pickup-zones',
            'dropoff_zones': '/api/top-dropoff-zones'
        }
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)