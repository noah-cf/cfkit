import os
import pandas as pd
from datetime import datetime
from shutil import copyfile
from math import ceil

def validate_input_file(file_path):
    """Validate the input file exists and is an Excel file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if not file_path.endswith(('.xlsx', '.xls')):
        raise ValueError("Input file must be an Excel file (.xlsx or .xls)")

def validate_num_splits(num_splits, total_rows):
    """Validate the number of splits is reasonable."""
    if not isinstance(num_splits, int) or num_splits <= 0:
        raise ValueError("Number of splits must be a positive integer")
    
    if num_splits > total_rows:
        raise ValueError(f"Number of splits ({num_splits}) cannot be greater than number of rows ({total_rows})")

def split_excel(input_file, num_splits):
    """
    Split an Excel file into a specified number of separate files.
    
    Args:
        input_file (str): Path to the input Excel file
        num_splits (int): Number of splits desired
    """
    try:
        validate_input_file(input_file)
        df = pd.read_excel(input_file)
        total_rows = len(df)
        
        validate_num_splits(num_splits, total_rows)
        
        base_rows_per_split = total_rows // num_splits
        remainder = total_rows % num_splits
        
        base_dir = os.path.dirname(input_file)
        current_date = datetime.now().strftime('%Y_%m_%d')
        script_name = 'excel_splitter'
        output_directory = os.path.join(base_dir, f'{current_date}_{script_name}_output')
        os.makedirs(output_directory, exist_ok=True)
        
        input_filename = os.path.splitext(os.path.basename(input_file))[0]
        file_extension = os.path.splitext(input_file)[1]
        
        input_copy = os.path.join(output_directory, f'{current_date}_{script_name}_input{file_extension}')
        copyfile(input_file, input_copy)
        
        start_idx = 0
        for i in range(num_splits):
            rows_this_split = base_rows_per_split + (1 if i < remainder else 0)
            end_idx = start_idx + rows_this_split
            
            split_df = df.iloc[start_idx:end_idx]
            
            output_filename = f"{input_filename}_{i+1}-{num_splits}{file_extension}"
            output_file = os.path.join(output_directory, output_filename)
            
            if file_extension == '.xlsx':
                split_df.to_excel(output_file, index=False)
            else:
                split_df.to_excel(output_file, index=False, engine='openpyxl')
            
            start_idx = end_idx
            print(f"Created split {i+1}/{num_splits}: {output_filename}")
        
        print(f"\nSplit completed successfully. Files saved to {output_directory}")
        print(f"Original file copied to {input_copy}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/small_tasks/excel_splitter"))
    
    input_files = [f for f in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, f))]
    supported_files = [f for f in input_files if f.endswith(('.xlsx', '.xls'))]
    
    if not supported_files:
        raise FileNotFoundError(f"No valid Excel files found in {base_dir}")
    
    input_file_path = os.path.join(base_dir, supported_files[0])
    
    while True:
        try:
            num_splits = int(input("Enter the number of splits desired: "))
            if num_splits > 0:
                break
            print("Please enter a positive number.")
        except ValueError:
            print("Please enter a valid integer.")
    
    split_excel(input_file_path, num_splits)

if __name__ == "__main__":
    main()