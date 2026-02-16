import pandas as pd

def convert_large_csv_to_excel(csv_path, output_excel):
    # Excel's absolute maximum rows per sheet
    EXCEL_LIMIT = 1000000 
    # How many rows to read into memory at once (adjust based on your RAM)
    CHUNK_SIZE = 50000 
    
    print(f"Reading {csv_path}...")
    
    sheet_num = 1
    current_sheet_row_count = 0
    accumulated_chunks = []
    total_rows_processed = 0
    
    # Read the CSV in chunks
    reader = pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False)
    
    for i, chunk in enumerate(reader):
        accumulated_chunks.append(chunk)
        current_sheet_row_count += len(chunk)
        total_rows_processed += len(chunk)
        
        # When we accumulate 1 million rows, write them to Excel and reset
        if current_sheet_row_count >= EXCEL_LIMIT:
            # Concatenate all accumulated chunks
            df_batch = pd.concat(accumulated_chunks, ignore_index=True)
            
            # Take only up to EXCEL_LIMIT rows
            df_to_write = df_batch.iloc[:EXCEL_LIMIT]
            
            sheet_name = f'Data_Part_{sheet_num}'
            
            print(f"Writing {len(df_to_write):,} rows to {sheet_name}...")
            
            # Write to Excel
            if sheet_num == 1:
                df_to_write.to_excel(output_excel, sheet_name=sheet_name, index=False)
            else:
                with pd.ExcelWriter(output_excel, engine='openpyxl', if_sheet_exists='new') as writer:
                    df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # If there are leftover rows, keep them for the next batch
            if len(df_batch) > EXCEL_LIMIT:
                accumulated_chunks = [df_batch.iloc[EXCEL_LIMIT:]]
                current_sheet_row_count = len(accumulated_chunks[0])
            else:
                accumulated_chunks = []
                current_sheet_row_count = 0
            
            sheet_num += 1
            print(f"Progress: ~{total_rows_processed:,} rows processed...")
    
    # Write any remaining data
    if accumulated_chunks:
        df_batch = pd.concat(accumulated_chunks, ignore_index=True)
        sheet_name = f'Data_Part_{sheet_num}'
        
        print(f"Writing final {len(df_batch):,} rows to {sheet_name}...")
        
        if sheet_num == 1:
            df_batch.to_excel(output_excel, sheet_name=sheet_name, index=False)
        else:
            with pd.ExcelWriter(output_excel, engine='openpyxl', if_sheet_exists='new') as writer:
                df_batch.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\nDone! Your file has been saved as: {output_excel}")
    print(f"Total rows processed: {total_rows_processed:,}")

# --- SETTINGS ---
input_csv = 'yellow_tripdata_2019-01.csv'  # <--- Put your filename here
output_xlsx = 'yellow_tripdata_2019.xlsx'

convert_large_csv_to_excel(input_csv, output_xlsx)