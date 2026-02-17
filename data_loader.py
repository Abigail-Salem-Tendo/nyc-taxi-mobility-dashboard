import pandas as pd
import json
import os
import geopandas as gpd
from shapely.geometry import mapping
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()


# DATABASE CONNECTION
def get_engine():
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
    print(f" Connected to: {DB_NAME}")
    return engine


# Loading the zone_lookup.csv file into zone_lookup table 
def load_zone_lookup(engine, csv_path):
    print("\n" + "="*60)
    print("STEP 1: LOADING ZONE LOOKUP")
    print("="*60)

    df = pd.read_csv(csv_path)
    print(f"  Read {len(df)} rows")

    # Rename columns to match database schema exactly
    df = df.rename(columns={
        'LocationID':   'location_id',
        'Borough':      'borough',
        'Zone':         'zone_name',
        'service_zone': 'service_zone'
    })

    # Only keep the 4 columns the table has
    df = df[['location_id', 'borough', 'zone_name', 'service_zone']]

    # insert into zone_lookup table
    df.to_sql('zone_lookup', engine, if_exists='append', index=False)
    print(f" inserted {len(df)} zones into zone_lookup")

# Loading the shapefile and inserting into zone_geo table
def load_zone_geo(engine, shapefile_path):
    print("\n" + "="*60)
    print("STEP 2: LOADING ZONE GEOMETRY")
    print("="*60)

    # Read shapefile
    gdf = gpd.read_file(shapefile_path)
    print(f"  Read {len(gdf)} geometries")
    print(f"  Columns found: {list(gdf.columns)}")

    # Build rows list
    rows = []
    for _, row in gdf.iterrows():

        # Try different possible column names for location_id
        location_id = None
        for col in ['location_i', 'LocationID', 'OBJECTID', 'location_id']:
            if col in row.index and row[col] is not None:
                location_id = int(row[col])
                break

        if location_id is None:
            print(f"    Skipping row - could not find location_id")
            continue

        # Convert geometry shape to GeoJSON string
        geojson_str = json.dumps(mapping(row['geometry']))

        rows.append({
            'location_id':  location_id,
            'zone_geojson': geojson_str
        })

    # Remove duplicates
    geo_df = pd.DataFrame(rows)
    geo_df = geo_df.drop_duplicates(subset=['location_id'])
    print(f"  {len(geo_df)} unique geometries after deduplication")

    # Only keep location_ids that exist in zone_lookup
    with engine.connect() as conn:
        result   = conn.execute(text("select location_id from zone_lookup"))
        valid_ids = set(row[0] for row in result)

    geo_df = geo_df[geo_df['location_id'].isin(valid_ids)]
    print(f"  {len(geo_df)} geometries match zones in zone_lookup")

    # insert one row at a time using raw SQL
    inserted = 0
    skipped  = 0

    with engine.connect() as conn:
        for _, row in geo_df.iterrows():
            try:
                conn.execute(text("""
                    insert into zone_geo (location_id, zone_geojson)
                    values (:location_id, :zone_geojson)
                """), {
                    'location_id':  int(row['location_id']),
                    'zone_geojson': row['zone_geojson']
                })
                inserted += 1
            except Exception as e:
                skipped += 1
                print(f"    Skipped location_id {row['location_id']}: {e}")
        conn.commit()

    print(f" inserted {inserted} geometries into zone_geo")
    if skipped > 0:
        print(f"  Skipped {skipped} geometries")


# Calculate isnight features for trip_data before inserting into database
def calculate_features(df):

    # Calculate trip duration in minutes
    df['trip_duration_min'] = (
        (df['dropoff_datetime'] - df['pickup_datetime'])
        .dt.total_seconds() / 60
    ).round(0).astype(int)

    # Hour of day (0-23) from pickup_datetime
    df['hour_of_day'] = df['pickup_datetime'].dt.hour

    # Getting the day of the week from pickup_datetime
    df['day_of_week'] = df['pickup_datetime'].dt.day_name()

    # Identification of peak hours where 7am-9am is morning-rush and 5pm-7pm is evening-rush
    peak_hours = [6, 7, 8, 9, 16, 17, 18, 19]
    df['is_peak_hour'] = df['pickup_datetime'].dt.hour.isin(peak_hours)

    # Average speed calculations by miles per hour (mph)
    duration_hours = df['trip_duration_min'] / 60
    df['avg_speed_mph'] = (df['trip_distance'] / duration_hours).round(2)

    # Determining congestion levels based on average speed calculations
    def get_congestion(speed):
        if speed < 10:    return 'High'
        elif speed <= 20: return 'Medium'
        else:             return 'Low'

    df['congestion_level'] = df['avg_speed_mph'].apply(get_congestion)

    return df


# Inserting the cleaned trip data into the trip_data table in batches to optimize performance
def load_trip_data(engine, csv_path, batch_size=100000):
    print("\n" + "="*60)
    print("STEP 3: LOADING TRIP DATA")
    print("="*60)
    print(f"  File:       {csv_path}")
    print(f"  Batch size: {batch_size:,} rows\n")

    # CSV column names → database column names
    rename_map = {
        'VendorID':              'vendor_id',
        'tpep_pickup_datetime':  'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'PULocationID':          'pulocation_id',
        'DOLocationID':          'dolocation_id'
    }

    # Exact column names in trip_data table
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

    for chunk in pd.read_csv(csv_path, chunksize=batch_size):
        chunk_num += 1
        print(f"  Chunk {chunk_num}: processing...")

        # Parse datetime columns
        chunk['tpep_pickup_datetime']  = pd.to_datetime(chunk['tpep_pickup_datetime'])
        chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'])

        # Rename to match database columns
        chunk = chunk.rename(columns=rename_map)

        # Calculate all 6 derived features
        chunk = calculate_features(chunk)

        # Keep only columns the database table has
        cols  = [c for c in db_columns if c in chunk.columns]
        chunk = chunk[cols]

        # insert into trip_data
        chunk.to_sql(
            'trip_data',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        total_inserted += len(chunk)
        print(f"  Chunk {chunk_num}:  {total_inserted:,} trips inserted so far")

    print(f"\n Total trips inserted: {total_inserted:,}")


# Inserting the exxluded data log from the cleaner script into the excluded_data_log table
def load_excluded_log(engine, csv_path):
    print("\n" + "="*60)
    print("STEP 4: LOADING EXCLUDED DATA LOG")
    print("="*60)

    if not os.path.exists(csv_path):
        print(f"  c  File not found: {csv_path}")
        print(f"  Skipping...")
        return

    df = pd.read_csv(csv_path)
    print(f"  Read {len(df)} log entries")

    # Only keep columns the table has
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
    print(f" inserted {len(df)} entries into excluded_data_log")


# Main function to run all steps in order
def main():
    print("\n" + "="*60)
    print("NYC TAXI DATA LOADER")
    print("="*60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get all file paths to the data sets from .env
    ZONE_LOOKUP  = os.getenv('ZONE_LOOKUP_FILE')
    ZONE_SHAPE   = os.getenv('ZONE_SHAPEFILE')
    CLEANED_DATA = os.getenv('CLEANED_TRIP_DATA')
    EXCLUDED_LOG = os.getenv('EXCLUDED_LOG_FILE')

    # Make sure required paths are in .env
    if not all([ZONE_LOOKUP, ZONE_SHAPE, CLEANED_DATA]):
        raise ValueError(
            "Missing paths in .env!\n"
            "Need: ZONE_LOOKUP_FILE, ZONE_SHAPEFILE, CLEANED_TRIP_DATA"
        )

    # Make sure files actually exist
    for path in [ZONE_LOOKUP, ZONE_SHAPE, CLEANED_DATA]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")

    try:
        # Connect to database
        engine = get_engine()

        # Load zone lookup and geometry data first since trip_data depends on it for foreign keys
        load_zone_lookup(engine, ZONE_LOOKUP)
        load_zone_geo(engine, ZONE_SHAPE)
        load_trip_data(engine, CLEANED_DATA)

        # Load excluded log if path is set
        if EXCLUDED_LOG:
            load_excluded_log(engine, EXCLUDED_LOG)

        print("\n" + "="*60)
        print(" ALL DATA LOADED SUCCESSFULLY!")
        print("="*60)
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    except FileNotFoundError as e:
        print(f"\n FILE ERROR: {e}")
        print("Check your .env file paths.")

    except RuntimeError as e:
        print(f"\n DATABASE ERROR: {e}")

    except Exception as e:
        print(f"\n ERROR: {e}")
        raise


if __name__ == "__main__":
    main()