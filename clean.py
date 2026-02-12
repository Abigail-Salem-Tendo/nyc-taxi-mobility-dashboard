import pandas as pd

def convert_large_csv_to_excel(csv_path, output_excel):
    # Excel's absolute maximum rows per sheet
    EXCEL_LIMIT = 1000000 
    # How many rows to read into memory at once (adjust based on your RAM)
    CHUNK_SIZE = 100000 
    
    print(f"Reading {csv_path}...")
    
    # Initialize the Excel Writer
    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        sheet_num = 1
        current_sheet_row_count = 0
        
        # Read the CSV in chunks
        reader = pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False)
        
        for i, chunk in enumerate(reader):
            # Check if adding this chunk exceeds the current sheet's limit
            if current_sheet_row_count + len(chunk) > EXCEL_LIMIT:
                print(f"Sheet {sheet_num} full. Starting Sheet {sheet_num + 1}...")
                sheet_num += 1
                current_sheet_row_count = 0
            
            sheet_name = f'Data_Part_{sheet_num}'
            
            # Write the chunk
            # If it's the start of a new sheet, write the header. Otherwise, append.
            chunk.to_excel(
                writer, 
                sheet_name=sheet_name, 
                index=False, 
                header=(current_sheet_row_count == 0),
                startrow=current_sheet_row_count
            )
            
            current_sheet_row_count += len(chunk)
            total_processed = ((sheet_num - 1) * EXCEL_LIMIT) + current_sheet_row_count
            
            if i % 5 == 0:
                print(f"Progress: ~{total_processed:,} rows processed...")

    print(f"\nDone! Your file has been saved as: {output_excel}")

# --- SETTINGS ---
input_csv = 'yellow_tripdata_2019-01.csv'  # <--- Put your filename here
output_xlsx = 'yellow_tripdata_2019.xlsx'

convert_large_csv_to_excel(input_csv, output_xlsx)