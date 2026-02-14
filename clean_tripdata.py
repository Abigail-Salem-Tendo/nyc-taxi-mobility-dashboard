import pandas as pd
import os

def clean_data_simple(input_file):
    output_file = "cleaned_data.csv"
    log_file = "cleaning_log.txt"
    
    # 1. Start fresh: Delete old files if they exist
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # Counters for our log
    total_in = 0
    total_out = 0

    print(f"Starting cleaning: {input_file}")

    # 2. Process in chunks to save memory
    reader = pd.read_csv(input_file, chunksize=100000)
    
    first_chunk = True
    for chunk in reader:
        total_in += len(chunk)

        # --- THE CLEANING RULES ---
        # Rule: No empty rows
        chunk = chunk.dropna(how='all')

        # Rule: Valid distance and passengers
        chunk = chunk[(chunk['trip_distance'] > 0) & 
                      (chunk['passenger_count'] > 0) & 
                      (chunk['passenger_count'] <= 6)]

        # Rule: No negative money
        chunk = chunk[chunk['fare_amount'] >= 0]

        # Rule: Dropoff must be after pickup
        chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'])
        chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'])
        chunk = chunk[chunk['tpep_dropoff_datetime'] > chunk['tpep_pickup_datetime']]

        # --- FEATURE ENGINEERING ---
        # Simple speed calculation
        duration_hours = (chunk['tpep_dropoff_datetime'] - chunk['tpep_pickup_datetime']).dt.total_seconds() / 3600
        chunk['avg_speed_mph'] = chunk['trip_distance'] / duration_hours.replace(0, 0.0001)

        # --- SAVE DATA ---
        # Write to CSV (header only for the first time)
        chunk.to_csv(output_file, mode='a', index=False, header=first_chunk)
        
        total_out += len(chunk)
        first_chunk = False
        print(f"Progress: {total_in} rows processed...")

    # 3. SAVE LOG FILE
    with open(log_file, "w") as f:
        f.write("CLEANING LOG\n")
        f.write(f"Source File: {input_file}\n")
        f.write(f"Total Rows Scanned: {total_in}\n")
        f.write(f"Total Rows Saved: {total_out}\n")
        f.write(f"Rows Deleted: {total_in - total_out}\n")

    print(f"Done! Cleaned file: {output_file}")
    print(f"Log report: {log_file}")

# To run:
clean_data_simple("yellow_tripdata_2019-01.csv")