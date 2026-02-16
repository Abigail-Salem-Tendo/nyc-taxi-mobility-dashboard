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

    # Load valid taxi zones for location validation
    print("Loading taxi zone lookup...")
    try:
        zones = pd.read_csv(zone_lookup_file)
        all_zone_ids = set(zones['LocationID'].tolist())
        unknown_zone_ids = 264
        valid_zone_ids = all_zone_ids - {unknown_zone_ids}
        print(f"  Found {len(valid_zone_ids)} valid taxi zones to be used for validation")
    except:
        print("ERROR: Could not load taxi_zone_lookup.csv")
        return

    # Counters for detailed logging
    total_in = 0
    total_out = 0
    issues = {
        'empty_rows': 0,
        'missing_critical_fields': 0,
        'invalid_datetime': 0,
        'negative_duration': 0,
        'zero_distance': 0,
        'distance_too_high': 0,
        'invalid_passengers': 0,
        'negative_fare': 0,
        'fare_too_high': 0,
        'duration_too_long': 0,
        'speed_too_high': 0,
        'speed_too_low': 0,
        'unknown_location': 0,
        'invalid_location': 0,
        'duplicates': 0
    }

    print(f"\nStarting cleaning: {input_file}\n")

    # Process in chunks to handle large files
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
                           'trip_distance', 'fare_amount', 'PULocationID', 'DOLocationID']

        # Remove rows with any missing critical field
        for field in critical_fields:
            if field in chunk.columns:
                before = len(chunk)
                chunk = chunk[chunk[field].notna()]
                issues['missing_critical_fields'] = issues['missing_critical_fields'] + (before - len(chunk))

        #Parse and validate datetimes

        chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'], errors='coerce')
        chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'], errors='coerce')

        # Remove rows where datetime parsing failed
        before = len(chunk)
        chunk = chunk[chunk['tpep_pickup_datetime'].notna()]
        chunk = chunk[chunk['tpep_dropoff_datetime'].notna()]
        issues['invalid_datetime'] = issues['invalid_datetime'] + (before - len(chunk))

        #Check dropoff is after pickup
        before = len(chunk)
        chunk = chunk[chunk['tpep_dropoff_datetime'] > chunk['tpep_pickup_datetime']]
        issues['negative_duration'] = issues['negative_duration'] + (before - len(chunk))

        #Remove duplicates
        before = len(chunk)
        chunk = chunk.drop_duplicates(
            subset=['tpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']
        )
        issues['duplicates'] = issues['duplicates'] + (before - len(chunk))

        # Remove zero or negative distance
        before = len(chunk)
        chunk = chunk[chunk['trip_distance'] > 0]
        issues['zero_distance'] = issues['zero_distance'] + (before - len(chunk))

        # Remove unrealistic long distances (NYC to Hartford CT ~ 100 miles)
        before = len(chunk)
        chunk = chunk[chunk['trip_distance'] <= 100]
        issues['distance_too_high'] = issues['distance_too_high'] + (before - len(chunk))

        # Validate passenger count
        # NYC taxis have max 6 passengers, minimum 1
        before = len(chunk)
        chunk = chunk[(chunk['passenger_count'] >= 1) & (chunk['passenger_count'] <= 6)]
        issues['invalid_passengers'] = issues['invalid_passengers'] + (before - len(chunk))

        # Remove negative fares
        before = len(chunk)
        chunk = chunk[chunk['fare_amount'] >= 0]
        issues['negative_fare'] = issues['negative_fare'] + (before - len(chunk))

        # Remove unrealistic high fares (airport trips ~$150 max normally)
        before = len(chunk)
        chunk = chunk[chunk['fare_amount'] <= 500]
        issues['fare_too_high'] = issues['fare_too_high'] + (before - len(chunk))

        # Calculate duration in hours
        duration_seconds = (chunk['tpep_dropoff_datetime'] - chunk['tpep_pickup_datetime']).dt.total_seconds()
        duration_hours = duration_seconds / 3600

        # Remove trips longer than 6 hours (likely data errors)
        before = len(chunk)
        chunk = chunk[duration_hours <= 6]
        duration_hours = duration_hours[duration_hours <= 6]
        issues['duration_too_long'] = issues['duration_too_long'] + (before - len(chunk))

        # STEP 9: Calculate average speed
        # Avoid division by zero
        duration_hours_safe = duration_hours.copy()
        duration_hours_safe[duration_hours_safe == 0] = 0.0001

        chunk['avg_speed_mph'] = chunk['trip_distance'] / duration_hours_safe

        # Validate speed (City Planning constraints)
        # Remove impossibly high speeds (>80 mph on NYC streets)
        before = len(chunk)
        chunk = chunk[chunk['avg_speed_mph'] <= 80]
        issues['speed_too_high'] = issues['speed_too_high'] + (before - len(chunk))

        # Remove suspiciously low speeds (<0.5 mph suggests parking/error)
        before = len(chunk)
        chunk = chunk[chunk['avg_speed_mph'] >= 0.5]
        issues['speed_too_low'] = issues['speed_too_low'] + (before - len(chunk))

        # Validate location IDs

        before = len(chunk)
        unknown_pickup = chunk['PULocationID'] == 264
        unknown_dropoff = chunk['DOLocationID'] == 264
        chunk = chunk[~(unknown_pickup | unknown_dropoff)]
        issues['unknown_location'] = issues['unknown_location'] + (before - len(chunk))

        # Check pickup location exists in valid zones
        before = len(chunk)
        chunk = chunk[chunk['PULocationID'].isin(valid_zone_ids)]
        issues['invalid_location'] = issues['invalid_location'] + (before - len(chunk))

        # Check dropoff location exists
        before = len(chunk)
        chunk = chunk[chunk['DOLocationID'].isin(valid_zone_ids)]
        issues['invalid_location'] = issues['invalid_location'] + (before - len(chunk))

        #Feature Engineering
        # 1. Trip duration in minutes
        chunk['trip_duration_min'] = (
            (chunk['tpep_dropoff_datetime'] - chunk['tpep_pickup_datetime']).dt.total_seconds() / 60
        ).astype(int)

        # 2. Hour of day (0-23)
        hour_list = []
        for dt in chunk['tpep_pickup_datetime']:
            hour_list.append(dt.hour)
        chunk['hour_of_day'] = hour_list

        # 3. Day of week (1=Sunday, 7=Saturday to match MySQL DAYOFWEEK)
        day_list = []
        for dt in chunk['tpep_pickup_datetime']:
            # Python weekday(): Monday=0, Sunday=6
            # MySQL DAYOFWEEK(): Sunday=1, Saturday=7
            python_weekday = dt.weekday()
            mysql_dayofweek = (python_weekday + 2) % 7
            if mysql_dayofweek == 0:
                mysql_dayofweek = 7
            day_list.append(mysql_dayofweek)
        chunk['day_of_week'] = day_list

        # 4. Is peak hour (7-9am, 5-7pm)
        peak_hours = {7, 8, 9, 17, 18, 19}
        peak_list = []
        for hour in chunk['hour_of_day']:
            if hour in peak_hours:
                peak_list.append(1)  # 1 for True (MySQL BOOLEAN)
            else:
                peak_list.append(0)  # 0 for False
        chunk['is_peak_hour'] = peak_list

        # 5. Congestion level (High/Medium/Low)
        congestion_list = []
        for speed in chunk['avg_speed_mph']:
            if speed < 10:
                congestion_list.append('High')
            elif speed <= 25:
                congestion_list.append('Medium')
            else:
                congestion_list.append('Low')
        chunk['congestion_level'] = congestion_list


        # =============================================
        # STEP 13: Save cleaned chunk
        # =============================================
        if len(chunk) > 0:
            chunk.to_csv(output_file, mode='a', index=False, header=first_chunk)
            total_out = total_out + len(chunk)
            first_chunk = False

        # Progress update
        percent_kept = (total_out / total_in * 100) if total_in > 0 else 0
        print(f"  Chunk {chunk_number}: {total_in:,} rows processed | {total_out:,} kept ({percent_kept:.1f}%)")

    # =============================================
    # SAVE DETAILED LOG FILE
    # =============================================
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
        f.write("3. Removed rows with invalid dates\n")
        f.write("4. Removed duplicate trips\n")
        f.write("5. Distance: Must be 0-100 miles\n")
        f.write("6. Passengers: Must be 1-6\n")
        f.write("7. Fare: Must be $0-$500\n")
        f.write("8. Duration: Must be 0-6 hours\n")
        f.write("9. Speed: Must be 0.5-80 mph\n")
        f.write("10. Locations: Must exist in taxi zone lookup\n")
        f.write("11. Dropoff must be after pickup\n\n")

        f.write("CONGESTION LEVEL LOGIC:\n")
        f.write("  High:   Speed < 10 mph (severe gridlock)\n")
        f.write("  Medium: Speed 10-25 mph (typical urban traffic)\n")
        f.write("  Low:    Speed > 25 mph (free flow)\n")

    # Save database log for excluded_data_log table

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

# RUN THE SCRIPT
if __name__ == "__main__":
    clean_data_simple(
        input_file="yellow_tripdata_2019-01.csv",
        zone_lookup_file="taxi_zone_lookup.csv"
    )