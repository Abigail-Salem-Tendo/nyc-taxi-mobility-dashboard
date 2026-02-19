import pandas as pd
import os


def clean_data_simple(input_file, zone_lookup_file):
    # Output files
    output_file = "cleaned_data.csv"
    log_file = "cleaning_log.txt"
    excluded_log_file = "excluded_data_log.csv"

    # Delete old files if they exist
    if os.path.exists(output_file):
        os.remove(output_file)
    if os.path.exists(excluded_log_file):
        os.remove(excluded_log_file)

    # LOAD VALID ZONES FOR VALIDATION
    print("Loading taxi zone lookup...")
    try:
        zones = pd.read_csv(zone_lookup_file)
        all_zone_ids = set(zones['LocationID'].tolist())

        # Exclude Zone 264 (Unknown) and Zone 265 (Outside of NYC)
        excluded_zones = {264, 265}
        valid_zone_ids = all_zone_ids - excluded_zones

        print(f"  Found {len(all_zone_ids)} total zones")
        print(f"  Using {len(valid_zone_ids)} valid zones (excluding Zones 264 & 265)")
        print(f"  Excluded: Zone 264 (Unknown), Zone 265 (Outside of NYC)")
    except:
        print("ERROR: Could not load taxi_zone_lookup.csv")
        return

    # TRACKING COUNTERS
    total_in = 0
    total_out = 0
    issues = {
        'empty_rows': 0,
        'missing_critical_fields': 0,
        'invalid_datetime': 0,
        'negative_duration': 0,
        'zero_duration': 0,
        'duration_too_long': 0,
        'zero_distance': 0,
        'invalid_passengers': 0,
        'invalid_fare': 0,
        'excluded_zones': 0,
        'invalid_location': 0
    }

    print(f"\nStarting cleaning: {input_file}\n")

    # PROCESS IN CHUNKS
    reader = pd.read_csv(input_file, chunksize=100000)
    first_chunk = True
    chunk_number = 0

    for chunk in reader:
        chunk_number = chunk_number + 1
        total_in = total_in + len(chunk)

        # STEP 1: Remove completely empty rows
        before = len(chunk)
        chunk = chunk.dropna(how='all')
        issues['empty_rows'] = issues['empty_rows'] + (before - len(chunk))

        # STEP 2: Check for missing critical fields
        critical_fields = ['tpep_pickup_datetime', 'tpep_dropoff_datetime',
                           'trip_distance', 'fare_amount', 'PULocationID',
                           'DOLocationID', 'total_amount', 'passenger_count']

        for field in critical_fields:
            if field in chunk.columns:
                before = len(chunk)
                chunk = chunk[chunk[field].notna()]
                issues['missing_critical_fields'] = issues['missing_critical_fields'] + (before - len(chunk))

        # Parse and validate datetimes
        chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'], errors='coerce')
        chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'], errors='coerce')

        # Remove rows where datetime parsing failed
        before = len(chunk)
        chunk = chunk[chunk['tpep_pickup_datetime'].notna()]
        chunk = chunk[chunk['tpep_dropoff_datetime'].notna()]
        issues['invalid_datetime'] = issues['invalid_datetime'] + (before - len(chunk))

        # Check dropoff is after pickup
        before = len(chunk)
        chunk = chunk[chunk['tpep_dropoff_datetime'] > chunk['tpep_pickup_datetime']]
        issues['negative_duration'] = issues['negative_duration'] + (before - len(chunk))

        # Validate trip duration (0 < duration <= 4 hours)
        # Calculate duration in hours for validation only
        duration_seconds = (chunk['tpep_dropoff_datetime'] - chunk['tpep_pickup_datetime']).dt.total_seconds()
        duration_hours = duration_seconds / 3600

        # Remove trips with zero duration
        before = len(chunk)
        chunk = chunk[duration_hours > 0]
        duration_hours = duration_hours[duration_hours > 0]
        issues['zero_duration'] = issues['zero_duration'] + (before - len(chunk))

        # Remove trips longer than 4 hours
        before = len(chunk)
        chunk = chunk[duration_hours <= 4]
        duration_hours = duration_hours[duration_hours <= 4]
        issues['duration_too_long'] = issues['duration_too_long'] + (before - len(chunk))

        # Validate trip distance
        # Remove zero or negative distance
        before = len(chunk)
        chunk = chunk[chunk['trip_distance'] > 0]
        issues['zero_distance'] = issues['zero_distance'] + (before - len(chunk))

        # Validate passenger count
        # Assuming NYC taxis have max 6 passengers, minimum 1
        before = len(chunk)
        chunk = chunk[(chunk['passenger_count'] >= 1) & (chunk['passenger_count'] <= 6)]
        issues['invalid_passengers'] = issues['invalid_passengers'] + (before - len(chunk))

        # Validate fare amount
        # Remove negative fares only
        before = len(chunk)
        chunk = chunk[chunk['fare_amount'] > 0]
        issues['invalid_fare'] = issues['invalid_fare'] + (before - len(chunk))

        # Exclude Zone 264 (Unknown) and Zone 265 (Outside of NYC)
        # These zones are invalid for NYC city planning analysis
        before = len(chunk)
        excluded_pickup = chunk['PULocationID'].isin([264, 265])
        excluded_dropoff = chunk['DOLocationID'].isin([264, 265])
        has_excluded = excluded_pickup | excluded_dropoff
        chunk = chunk[~has_excluded]
        issues['excluded_zones'] = issues['excluded_zones'] + (before - len(chunk))

        # Check pickup location exists in valid zones
        before = len(chunk)
        chunk = chunk[chunk['PULocationID'].isin(valid_zone_ids)]
        issues['invalid_location'] = issues['invalid_location'] + (before - len(chunk))

        # Check dropoff location exists in valid zones
        before = len(chunk)
        chunk = chunk[chunk['DOLocationID'].isin(valid_zone_ids)]
        issues['invalid_location'] = issues['invalid_location'] + (before - len(chunk))

        #  Save cleaned chunk
        if len(chunk) > 0:
            chunk.to_csv(output_file, mode='a', index=False, header=first_chunk)
            total_out = total_out + len(chunk)
            first_chunk = False

        # Progress update
        percent_kept = (total_out / total_in * 100) if total_in > 0 else 0
        print(f"  Chunk {chunk_number}: {total_in:,} rows processed | {total_out:,} kept ({percent_kept:.1f}%)")

    # SAVE DETAILED LOG FILE
    with open(log_file, "w") as f:
        f.write("=" * 70 + "\n")
        f.write("NYC TAXI DATA CLEANING LOG\n")
        f.write("City Planning & Urban Mobility Analysis\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Source File: {input_file}\n")
        f.write(f"Output File: {output_file}\n\n")

        f.write(f"Total Rows Scanned:    {total_in:>12,}\n")
        f.write(f"Total Rows Kept:       {total_out:>12,}\n")
        f.write(f"Total Rows Excluded:   {total_in - total_out:>12,}\n")
        f.write(f"Success Rate:          {total_out / total_in * 100:>11.1f}%\n\n")

        f.write("=" * 70 + "\n")
        f.write("ISSUES FOUND\n")
        f.write("=" * 70 + "\n")

        for issue_name in issues:
            count = issues[issue_name]
            if count > 0:
                percent = (count / total_in * 100) if total_in > 0 else 0
                issue_label = issue_name.replace('_', ' ').title()
                f.write(f"{issue_label:<30} {count:>10,}  ({percent:>5.2f}%)\n")

        f.write("\n" + "=" * 70 + "\n")
        f.write("CLEANING RULES APPLIED\n")
        f.write("=" * 70 + "\n")
        f.write("1. Removed empty rows\n")
        f.write("2. Removed rows with missing critical fields\n")
        f.write("3. Validated and parsed datetime formats\n")
        f.write("4. Ensured dropoff after pickup\n")
        f.write("5. Duration: Must be > 0 hours and <= 4 hours\n")
        f.write("6. Distance: Must be > 0 (no upper limit)\n")
        f.write("7. Passengers: Must be 1-6 (NYC taxi capacity)\n")
        f.write("8. Fare: Must be >= 0 (no upper limit)\n")
        f.write("9. Excluded Zone 264 (Unknown - no location data)\n")
        f.write("10. Excluded Zone 265 (Outside of NYC - not in jurisdiction)\n")
        f.write("11. Validated locations against taxi zone lookup\n\n")

        f.write("DESIGN DECISIONS:\n")
        f.write("- No duplicate removal: Concurrent identical trips are legitimate\n")
        f.write("- No distance cap: Long-distance trips are valid mobility patterns\n")
        f.write("- No fare cap: High fares legitimate for extended trips\n")
        f.write("- Duration: 0-4 hours focuses on typical urban mobility\n")
        f.write("- Zones 264 & 265 excluded: Invalid/outside NYC boundaries\n\n")

        f.write("FEATURE ENGINEERING:\n")
        f.write("ALL derived features calculated in loading script:\n")
        f.write("- avg_speed_mph (trip_distance / duration_hours)\n")
        f.write("- congestion_level (High/Medium/Low based on speed)\n")
        f.write("- trip_duration_min (duration in minutes)\n")
        f.write("- hour_of_day (0-23)\n")
        f.write("- day_of_week (1=Sunday, 7=Saturday)\n")
        f.write("- is_peak_hour (7-9am, 5-7pm)\n")

    # SAVE DATABASE LOG
    db_log_rows = []
    for issue_name in issues:
        count = issues[issue_name]
        if count > 0:
            db_log_rows.append({
                'issue_type': issue_name,
                'trip_identifier': 'bulk_cleaning',
                'field_name': issue_name.split('_')[0],
                'issue_description': f"{count:,} records excluded due to {issue_name.replace('_', ' ')}",
                'action_taken': 'excluded'
            })

    if len(db_log_rows) > 0:
        db_log_df = pd.DataFrame(db_log_rows)
        db_log_df.to_csv(excluded_log_file, index=False)

    # FINAL SUMMARY
    print("\n" + "=" * 70)
    print("CLEANING COMPLETE")
    print("=" * 70)
    print(f"\nResults:")
    print(f"  Cleaned data:  {output_file}")
    print(f"  Summary log:   {log_file}")
    print(f"  Database log:  {excluded_log_file}")
    print(f"\nKept {total_out:,} of {total_in:,} records ({total_out / total_in * 100:.1f}%)\n")
    print("Note: All feature calculations will occur during database loading")
    print("=" * 70 + "\n")


# RUN THE SCRIPT
if __name__ == "__main__":
    clean_data_simple(
        input_file=r"C:\Users\user\Documents\Summative\raw\yellow_tripdata_2019-01.csv",
        zone_lookup_file=r"C:\Users\user\Documents\Summative\raw\taxi_zone_lookup.csv"
    )