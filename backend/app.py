from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, func, text, case
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

#Endpoint about comparison between peak and off-peak hours
@app.route('/api/peak-vs-offpeak')
def peak_vs_offpeak():
    results = db.session.query(
        Tripdata.is_peak_hour,
        func.count(Tripdata.trip_id).label('trip_count'),
        func.round(func.avg(Tripdata.avg_speed_mph), 2).label('avg_speed_mph'),
        func.round(func.avg(Tripdata.fare_per_mile), 2).label('avg_fare_per_mile'),
        func.round(func.avg(Tripdata.fare_amount), 2).label('avg_fare'),
        func.round(func.avg(Tripdata.tip_amount), 2).label('avg_tip'),
        func.round(func.avg(Tripdata.trip_duration_min), 1).label('avg_duration_min'),
        func.round(func.avg(Tripdata.trip_distance), 2).label('avg_distance_miles'),
        func.round(func.avg(Tripdata.passenger_count), 2).label('avg_passengers'),
        func.round(func.sum(Tripdata.fare_amount), 2).label('total_revenue')
    ).group_by(
        Tripdata.is_peak_hour
    ).order_by(
        Tripdata.is_peak_hour
    ).all()
    
    return jsonify([{
        'is_peak_hour': r.is_peak_hour,
        'trip_count': r.trip_count,
        'avg_speed_mph': float(r.avg_speed_mph) if r.avg_speed_mph else 0,
        'avg_fare_per_mile': float(r.avg_fare_per_mile) if r.avg_fare_per_mile else 0,
        'avg_fare': float(r.avg_fare) if r.avg_fare else 0,
        'avg_tip': float(r.avg_tip) if r.avg_tip else 0,
        'avg_duration_min': float(r.avg_duration_min) if r.avg_duration_min else 0,
        'avg_distance_miles': float(r.avg_distance_miles) if r.avg_distance_miles else 0,
        'avg_passengers': float(r.avg_passengers) if r.avg_passengers else 0,
        'total_revenue': float(r.total_revenue) if r.total_revenue else 0
    } for r in results])

# function to count zones manually without using pandas value_counts or groupby

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
        # Find maximum in remaining unsorted portion
        max_index = i
        
        for j in range(i + 1, n):
            # Compare counts (second element in tuple)
            if zone_list[j][1] > zone_list[max_index][1]:
                max_index = j
        
        # Swap current position with maximum found
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

@app.route('/api/trips-by-distance', methods=['GET'])
def trips_by_distance():
    borough = request.args.get('borough')

    distance_category = case(a
        (Tripdata.trip_distance <= 2, "0-2"),
        (Tripdata.trip_distance <= 5, "2-5"),
        (Tripdata.trip_distance <= 10, "5-10"),
        else_="10+"
    ).label("distance_category")

    query = db.session.query(
        distance_category,
        func.count(Tripdata.trip_id).label("trip_count"),
        func.avg(Tripdata.fare_amount).label("avg_fare"),
        func.avg(Tripdata.trip_duration_min).label("avg_duration"),
        func.avg(Tripdata.avg_speed_mph).label("avg_speed")
    )

    # only join if borough filter is used
    if borough:
        query = query.join(
            ZoneLookup,
            Tripdata.pulocation_id == ZoneLookup.location_id
        ).filter(ZoneLookup.borough == borough)

    results = query.group_by(distance_category).all()

    data = []
    for row in results:
        data.append({
            "distance_category": row.distance_category,
            "trip_count": int(row.trip_count),
            "avg_fare": float(row.avg_fare) if row.avg_fare else 0,
            "avg_duration_min": float(row.avg_duration) if row.avg_duration else 0,
            "avg_speed_mph": float(row.avg_speed) if row.avg_speed else 0
        })

    return jsonify(data)

# ENDPOINT 2
@app.route('/api/short-trip-inefficiency', methods=['GET'])
def short_trip_inefficiency():
    max_distance = request.args.get('max_distance', 1.0, type=float)
    borough = request.args.get('borough')

    # Summary stats
    summary_query = db.session.query(
        func.count(Tripdata.trip_id).label("short_trip_count"),
        func.avg(Tripdata.fare_amount).label("avg_fare"),
        func.avg(Tripdata.trip_duration_min).label("avg_duration"),
        func.avg(Tripdata.avg_speed_mph).label("avg_speed")
    ).filter(Tripdata.trip_distance < max_distance)

    if borough:
        summary_query = summary_query.join(
            ZoneLookup,
            Tripdata.pulocation_id == ZoneLookup.location_id
        ).filter(ZoneLookup.borough == borough)

    summary = summary_query.first()

    # Top pickup zones for short trips
    top_zones_query = db.session.query(
        ZoneLookup.borough,
        ZoneLookup.zone_name,
        func.count(Tripdata.trip_id).label("count")
    ).join(
        Tripdata, Tripdata.pulocation_id == ZoneLookup.location_id
    ).filter(
        Tripdata.trip_distance < max_distance
    )

    if borough:
        top_zones_query = top_zones_query.filter(ZoneLookup.borough == borough)

    top_zones = top_zones_query.group_by(
        ZoneLookup.borough, ZoneLookup.zone_name
    ).order_by(
        func.count(Tripdata.trip_id).desc()
    ).limit(10).all()

    return jsonify({
        "filters": {
            "max_distance": max_distance,
            "borough": borough if borough else "All"
        },
        "summary": {
            "short_trip_count": int(summary.short_trip_count),
            "avg_fare": float(summary.avg_fare) if summary.avg_fare else 0,
            "avg_duration_min": float(summary.avg_duration) if summary.avg_duration else 0,
            "avg_speed_mph": float(summary.avg_speed) if summary.avg_speed else 0
        },
        "top_pickup_zones": [
            {
                "borough": z.borough,
                "zone_name": z.zone_name,
                "short_trip_count": int(z.count)
            }
            for z in top_zones
        ]
    })

@app.route('/api/borough-comparison', methods=['GET'])
def borough_comparison():
    results = db.session.query(
        ZoneLookup.borough,
        func.count(Tripdata.trip_id).label("total_trips"),
        func.avg(Tripdata.trip_distance).label("avg_distance"),
        func.avg(Tripdata.fare_amount).label("avg_fare"),
        func.avg(Tripdata.avg_speed_mph).label("avg_speed")
    ).join(
        ZoneLookup, Tripdata.pulocation_id == ZoneLookup.location_id
    ).group_by(
        ZoneLookup.borough
    ).order_by(
        func.count(Tripdata.trip_id).desc()
    ).all()

    data = []
    for row in results:
        data.append({
            "borough": row.borough,
            "total_trips": int(row.total_trips),
            "avg_distance": float(row.avg_distance) if row.avg_distance else 0,
            "avg_fare": float(row.avg_fare) if row.avg_fare else 0,
            "avg_speed_mph": float(row.avg_speed) if row.avg_speed else 0
        })
    return jsonify(data)
@app.route('/api/top-pickup-zones', methods=['GET'])
def top_pickup_zones():
  
   
    try:
        # Get limit parameter (default to 10, max 50)
        limit = request.args.get('limit', 10, type=int)
        if limit < 1 or limit > 50:
            limit = 10
        
        # Fetching list of zone IDs 
        trips = Tripdata.query.with_entities(Tripdata.pulocation_id).all()
        
        # Extract zone IDs into a simple list
        zone_ids = [trip.pulocation_id for trip in trips]
        total_trips = len(zone_ids)
        
        # STEP 1: Count using MANUAL algorithm (no .value_counts())
        zone_counts = count_zones_manual(zone_ids)
        
        # STEP 2: Convert dictionary to list of tuples for sorting
        zone_list = [(zone_id, count) for zone_id, count in zone_counts.items()]
        
        # STEP 3: Sort using MANUAL selection sort (no .sort())
        sorted_zones = selection_sort_zones(zone_list)
        
        # STEP 4: Get top N zones
        top_zones = sorted_zones[:limit]
        
        # format for a JSON response
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
        # Get limit parameter (default to 10, max 50)
        limit = request.args.get('limit', 10, type=int)
        if limit < 1 or limit > 50:
            limit = 10
        
        # Fetch all dropoff zone IDs using ORM
        trips = Tripdata.query.with_entities(Tripdata.dolocation_id).all()
        
        # Extract zone IDs into a simple list
        zone_ids = [trip.dolocation_id for trip in trips]
        total_trips = len(zone_ids)
        
        # STEP 1: Count using MANUAL algorithm
        zone_counts = count_zones_manual(zone_ids)
        
        # STEP 2: Convert to list for sorting
        zone_list = [(zone_id, count) for zone_id, count in zone_counts.items()]
        
        # STEP 3: Sort using MANUAL selection sort
        sorted_zones = selection_sort_zones(zone_list)
        
        # STEP 4: Get top N zones
        top_zones = sorted_zones[:limit]
        
        # STEP 5: Format response
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


@app.route('/api/algorithm-info', methods=['GET'])
def algorithm_info():
  
    return jsonify({
        'success': True,
        'project': 'NYC Taxi Mobility Dashboard',
        'algorithms': [
            {
                'name': 'Manual Counting Algorithm',
                'purpose': 'Count trips per zone',
                'time_complexity': 'O(n) this iterates through all trips once',
                'space_complexity': 'O(m) this stores only unique zones and their counts',
                'pseudo_code': [
                    '1. Create empty dictionary zone_counts',
                    '2. For each zone_id in dataset:',
                    '   a. If zone_id exists in dictionary:',
                    '      - Increment count by 1',
                    '   b. Else:',
                    '      - Add zone_id with count = 1',
                    '3. Return zone_counts dictionary'
                ],
            },
            {
                'name': 'Selection Sort',
                'purpose': 'Sort zones by trip count from highest to lowest',
                'time_complexity': 'O(n²) uses nestd loops to find maximum value for each position',
                'space_complexity': 'O(1) sorts in place without extra data structures',
                'pseudo_code': [
                    '1. For each position i from 0 to n-1:',
                    '   a. Set max_index = i (assume current has maximum)',
                    '   b. For each position j from i+1 to n:',
                    '      - If value at j > value at max_index:',
                    '        * Update max_index = j',
                    '   c. Swap element at position i with element at max_index',
                    '2. Return sorted list'
                ],
                'how_it_works': 'Repeatedly finds the maximum element and moves it to the front',
    
            }
        ],
        'implementation_details': {
            'no_libraries_used': [
                'pandas .groupby()',
                'pandas .value_counts()',
                'built-in sort() or sorted()',
                'collections.Counter',
                'heapq'
            ],
            'only_uses': [
                'Basic Python dictionaries',
                'Lists',
                'For loops',
                'Comparisons'
            ],
           
        },
        'real_world_application': {
            'problem': 'Which taxi zones are busiest?',
            'solution': 'Use algorithms to identify high-demand zones',
            'benefit': 'City planners can optimize taxi dispatch and reduce passenger wait times',
            'use_cases': [
                'Identify zones needing more taxi coverage',
                'Plan taxi dispatch routes efficiently',
                'Analyze pickup vs dropoff patterns',
                'Improve urban mobility planning'
            ]
        },
        'endpoints_using_algorithms': {
            'pickup_zones': '/api/top-pickup-zones',
            'dropoff_zones': '/api/top-dropoff-zones'
        }
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)