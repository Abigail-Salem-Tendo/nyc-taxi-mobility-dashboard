import pandas as pd
import json
import os
import geopandas as gpd
from shapely.geometry import mapping
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

# Load credentials from .env file in the same folder as this script
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# DATABASE CONNECTION
def get_engine():
    """
    Reads database credentials from .env and returns a SQLAlchemy engine.
    Assumes the database and tables already exist (run setup_db.py first).
    """
    DB_USER     = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST     = os.getenv('DB_HOST', 'localhost')
    DB_PORT     = os.getenv('DB_PORT', '3306')
    DB_NAME     = os.getenv('DB_NAME')

    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        raise ValueError(
            "Missing DB credentials in .env\n"
            "Need: DB_USER, DB_PASSWORD, DB_NAME"
        )

    url = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(url)
    print(f"  Connected to: {DB_NAME}")
    return engine

# LOAD ZONE LOOKUP
def load_zone_lookup(engine, csv_path):

    print("\n" + "="*60)
    print("STEP 1: LOADING ZONE LOOKUP")
    print("="*60)

    df = pd.read_csv(csv_path)
    print(f"  Read {len(df)} rows")

    # Rename CSV columns to match the exact column names in the database table
    df = df.rename(columns={
        'LocationID':   'location_id',
        'Borough':      'borough',
        'Zone':         'zone_name',
        'service_zone': 'service_zone'
    })

    # Drop any extra columns the CSV might have
    df = df[['location_id', 'borough', 'zone_name', 'service_zone']]

    df.to_sql('zone_lookup', engine, if_exists='append', index=False)
    print(f"  Inserted {len(df)} zones into zone_lookup")

# STEP 2: ZONE GEOMETRY
def load_zone_geo(engine, shapefile_path):
    """
    Reads the NYC taxi zone shapefile and inserts each zone's polygon geometry
    into the zone_geo table as a GeoJSON string.
    Only inserts zones that already exist in zone_lookup (respects foreign key).
    """
    print("\n" + "="*60)
    print("STEP 2: LOADING ZONE GEOMETRY")
    print("="*60)

    # geopandas reads shapefiles and gives us a DataFrame with a geometry column
    gdf = gpd.read_file(shapefile_path)
    print(f"  Read {len(gdf)} geometries")
    print(f"  Columns found: {list(gdf.columns)}")

    rows = []
    for _, row in gdf.iterrows():

        # The shapefile may use different column names for the location ID
        # so we try several possibilities in order of preference
        location_id = None
        for col in ['location_i', 'LocationID', 'OBJECTID', 'location_id']:
            if col in row.index and row[col] is not None:
                location_id = int(row[col])
                break

        if location_id is None:
            print(f"    Skipping row - could not find location_id")
            continue

        # Convert the shapely geometry object to a GeoJSON string
        # so it can be stored as text in MySQL
        geojson_str = json.dumps(mapping(row['geometry']))
        rows.append({'location_id': location_id, 'zone_geojson': geojson_str})

    # Build a DataFrame and remove any duplicate location IDs
    geo_df = pd.DataFrame(rows)
    geo_df = geo_df.drop_duplicates(subset=['location_id'])
    print(f"  {len(geo_df)} unique geometries after deduplication")

    # Only keep rows whose location_id exists in zone_lookup (foreign key safety check)
    with engine.connect() as conn:
        result    = conn.execute(text("SELECT location_id FROM zone_lookup"))
        valid_ids = set(row[0] for row in result)

    geo_df = geo_df[geo_df['location_id'].isin(valid_ids)]
    print(f"  {len(geo_df)} geometries match zones in zone_lookup")

    # Insert one row at a time so a single bad row doesn't block the whole batch
    inserted = 0
    skipped  = 0

    with engine.connect() as conn:
        for _, row in geo_df.iterrows():
            try:
                conn.execute(text("""
                    INSERT INTO zone_geo (location_id, zone_geojson)
                    VALUES (:location_id, :zone_geojson)
                """), {
                    'location_id':  int(row['location_id']),
                    'zone_geojson': row['zone_geojson']
                })
                inserted += 1
            except Exception as e:
                skipped += 1
                print(f"    Skipped location_id {row['location_id']}: {e}")
        conn.commit()

    print(f"  Inserted {inserted} geometries into zone_geo")
    if skipped > 0:
        print(f"  Skipped {skipped} geometries")

# FEATURE ENGINEERING
def calculate_features(df):
    # Trip duration
    # Subtract pickup from dropoff to get a timedelta, then convert to minutes
    df['trip_duration_min'] = (
        (df['dropoff_datetime'] - df['pickup_datetime'])
        .dt.total_seconds() / 60
    ).round(0).astype(int)

    # Remove trips with zero or negative duration (bad data that would cause division by zero)
    df = df[df['trip_duration_min'] > 0]

    # Time-based features
    df['hour_of_day'] = df['pickup_datetime'].dt.hour        # Integer 0-23
    df['day_of_week'] = df['pickup_datetime'].dt.day_name()  # e.g. 'Monday'

    # Peak hours: 6am-9am (morning rush) and 4pm-7pm (evening rush)
    peak_hours = [6, 7, 8, 9, 16, 17, 18, 19]
    # Cast to int (0/1) so MySQL can store it — Python booleans cause SQLAlchemy errors
    df['is_peak_hour'] = df['pickup_datetime'].dt.hour.isin(peak_hours).astype(int)

    # Average speed
    duration_hours = df['trip_duration_min'] / 60

    # Replace zero durations with NaN before dividing to avoid inf values,
    # then fill resulting NaN speeds with 0.0 so MySQL doesn't reject them
    df['avg_speed_mph'] = (
        df['trip_distance'] / duration_hours.replace(0, float('nan'))
    ).round(2).fillna(0.0)

    # Congestion level
    def get_congestion(speed):
        if speed < 10:
            return 'High'
        elif speed <= 20:
            return 'Medium'
        else:
            return 'Low'

    df['congestion_level'] = df['avg_speed_mph'].apply(get_congestion)

    return df

# STEP 3: TRIP DATA
def load_trip_data(engine, csv_path, batch_size=100000):
    print("\n" + "="*60)
    print("STEP 3: LOADING TRIP DATA")
    print("="*60)
    print(f"  File:       {csv_path}")
    print(f"  Batch size: {batch_size:,} rows\n")

    # Maps CSV column names to database column names
    rename_map = {
        'VendorID':              'vendor_id',
        'tpep_pickup_datetime':  'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'PULocationID':          'pulocation_id',
        'DOLocationID':          'dolocation_id'
    }

    # The exact column names expected in the trip_data table
    db_columns = [
        'vendor_id',
        'pickup_datetime',
        'dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'pulocation_id',
        'dolocation_id',
        'fare_amount',
        'tip_amount',
        'total_amount',
        'avg_speed_mph',
        'congestion_level',
        'trip_duration_min',
        'hour_of_day',
        'day_of_week',
        'is_peak_hour'
    ]

    total_inserted = 0
    chunk_num      = 0

    # pd.read_csv with chunksize returns an iterator — each loop gives
    # us the next batch_size rows rather than loading the whole file at once
    for chunk in pd.read_csv(csv_path, chunksize=batch_size):
        chunk_num += 1
        print(f"  Chunk {chunk_num}: processing...")

        # Parse datetime strings into proper datetime objects
        chunk['tpep_pickup_datetime']  = pd.to_datetime(chunk['tpep_pickup_datetime'])
        chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'])

        chunk = chunk.rename(columns=rename_map)
        #Calculate features
        chunk = calculate_features(chunk)

        # Drop any columns not in the database table
        cols  = [c for c in db_columns if c in chunk.columns]
        chunk = chunk[cols]

        #Insert into trip_data
        chunk.to_sql(
            'trip_data',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        total_inserted += len(chunk)
        print(f"  Chunk {chunk_num}: {total_inserted:,} trips inserted so far")

    print(f"\n  Total trips inserted: {total_inserted:,}")

# STEP 4: EXCLUDED DATA LOG
def load_excluded_log(engine, csv_path):
    print("\n" + "="*60)
    print("STEP 4: LOADING EXCLUDED DATA LOG")
    print("="*60)

    if not os.path.exists(csv_path):
        print(f"  File not found: {csv_path}")
        print(f"  Skipping...")
        return

    df = pd.read_csv(csv_path)
    print(f"  Read {len(df)} log entries")

    db_columns = [
        'issue_type',
        'trip_identifier',
        'field_name',
        'issue_description',
        'action_taken'
    ]
    cols = [c for c in db_columns if c in df.columns]
    df   = df[cols]

    df.to_sql('excluded_data_log', engine, if_exists='append', index=False)
    print(f"  Inserted {len(df)} entries into excluded_data_log")

# MAIN
def main():
    print("\n" + "="*60)
    print("NYC TAXI DATA LOADER")
    print("="*60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  NOTE: Run setup_db.py first if you haven't already.\n")

    ZONE_LOOKUP  = os.getenv('ZONE_LOOKUP_FILE')
    ZONE_SHAPE   = os.getenv('ZONE_SHAPEFILE')
    CLEANED_DATA = os.getenv('CLEANED_TRIP_DATA')
    EXCLUDED_LOG = os.getenv('EXCLUDED_LOG_FILE')

    if not all([ZONE_LOOKUP, ZONE_SHAPE, CLEANED_DATA]):
        raise ValueError(
            "Missing paths in .env!\n"
            "Need: ZONE_LOOKUP_FILE, ZONE_SHAPEFILE, CLEANED_TRIP_DATA"
        )

    for path in [ZONE_LOOKUP, ZONE_SHAPE, CLEANED_DATA]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")

    try:
        engine = get_engine()

        # Zone tables must be loaded first — trip_data has foreign keys to them
        load_zone_lookup(engine, ZONE_LOOKUP)
        load_zone_geo(engine, ZONE_SHAPE)
        load_trip_data(engine, CLEANED_DATA)

        if EXCLUDED_LOG:
            load_excluded_log(engine, EXCLUDED_LOG)

        print("\n" + "="*60)
        print(" ALL DATA LOADED SUCCESSFULLY!")
        print("="*60)
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    except FileNotFoundError as e:
        print(f"\n FILE ERROR: {e}")
        print("  Check your .env file paths.")

    except Exception as e:
        print(f"\n ERROR: {e}")
        raise

if __name__ == "__main__":
    main()