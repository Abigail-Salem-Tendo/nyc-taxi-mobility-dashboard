from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

#import models
from models import my_db, Tripdata, ZoneLookup

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

# Initialize SQLAlchemy with app
my_db.init_app(app)






# Algorithm endpoints using manual implementation

def count_zones_manual(zone_data):
    """
    MANUAL COUNTING ALGORITHM
    Count trips per zone WITHOUT using pandas groupby or value_counts.
    
    TIME COMPLEXITY: O(n) where n = number of trip records
    SPACE COMPLEXITY: O(m) where m = number of unique zones
    """
    zone_counts = {}
    
    for zone_id in zone_data:
        if zone_id in zone_counts:
            zone_counts[zone_id] = zone_counts[zone_id] + 1
        else:
            zone_counts[zone_id] = 1
    
    return zone_counts


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
    
# the endpoints below implement the algorithms for counting and sorting zones

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
        'project_info': {
            'title': 'NYC Taxi Trip Analysis with Custom Algorithms',
            'purpose': 'Analyze taxi trip patterns to help city planners optimize taxi dispatch',
            'student': 'Database Systems Course Project'
        },
        'algorithms': [
            {
                'name': 'Manual Counting Algorithm',
                'purpose': 'Count trips per zone without using pandas groupby or value_counts',
                'time_complexity': 'O(n)',
                'space_complexity': 'O(m)',
                'complexity_explanation': {
                    'n': 'number of trip records in database (~500,000+)',
                    'm': 'number of unique zones in NYC (~263 zones)',
                    'why_efficient': 'Linear time - we look at each trip exactly once'
                },
                'pseudo_code': [
                    '1. Create empty dictionary zone_counts',
                    '2. For each zone_id in dataset:',
                    '   a. If zone_id exists in dictionary:',
                    '      - Increment count by 1',
                    '   b. Else:',
                    '      - Add zone_id with count = 1',
                    '3. Return zone_counts dictionary'
                ],
                'why_manual': 'Demonstrates understanding of counting without relying on built-in functions'
            },
            {
                'name': 'Selection Sort',
                'purpose': 'Sort zones by trip count from highest to lowest without using built-in sort',
                'time_complexity': 'O(n²)',
                'space_complexity': 'O(1)',
                'complexity_explanation': {
                    'n': 'number of zones to sort (~263 zones)',
                    'why_acceptable': "O(n²) is fine for small datasets. With 263 zones, that's only ~34,500 comparisons",
                    'alternative': 'For larger datasets (10,000+ items), would use QuickSort or MergeSort with O(n log n)'
                },
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
                'why_manual': 'Shows understanding of sorting logic step-by-step'
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
            'code_location': 'app.py lines 67-145'
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
            'pickup_zones': {
                'url': '/api/top-pickup-zones',
                'method': 'GET',
                'parameters': 'limit (optional, default=10, max=50)',
                'example': '/api/top-pickup-zones?limit=5'
            },
            'dropoff_zones': {
                'url': '/api/top-dropoff-zones',
                'method': 'GET',
                'parameters': 'limit (optional, default=10, max=50)',
                'example': '/api/top-dropoff-zones?limit=5'
            }
        },
        'learning_outcomes': [
            'Understanding time and space complexity',
            'Implementing algorithms from scratch',
            'Choosing appropriate algorithms for dataset size',
            'Translating real-world problems into algorithmic solutions'
        ]
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)