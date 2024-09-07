import os
import pandas as pd
from shutil import copyfile
from datetime import datetime

def merge_duplicates(input_file, output_file):
    column_to_merge = input("What column is to be merged? (Use column letter, e.g., 'A'): ").upper()
    delimiter = input("What is the delimiter to merge on? ")

    file_extension = os.path.splitext(input_file)[1].lower()
    if file_extension == '.xlsx':
        df = pd.read_excel(input_file)
    elif file_extension == '.csv':
        df = pd.read_csv(input_file)
    else:
        raise ValueError(f"Unsupported file type: {file_extension}. Only .xlsx and .csv are supported.")

    column_name = df.columns[ord(column_to_merge) - ord('A')]
    group_columns = [col for col in df.columns if col != column_name]
    df_merged = df.groupby(group_columns, as_index=False).agg(lambda x: delimiter.join(x.astype(str).unique()) if x.name == column_name else x.iloc[0])

    if file_extension == '.xlsx':
        df_merged.to_excel(output_file, index=False)
    elif file_extension == '.csv':
        df_merged.to_csv(output_file, index=False)
    
    print(f"Duplicates have been merged and saved to {output_file}")

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/small_tasks/merge_with_delimiter"))
input_files = [f for f in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, f))]
supported_files = [f for f in input_files if f.endswith(('.xlsx', '.csv'))]

if not supported_files:
    raise FileNotFoundError(f"No valid .xlsx or .csv input file found in {base_dir}")

input_file_name = supported_files[0]
input_file_path = os.path.join(base_dir, input_file_name)

file_extension = os.path.splitext(input_file_name)[1].lower()

current_date = datetime.now().strftime('%Y_%m_%d')

script_name = 'merge_with_delimiter'

output_directory = os.path.join(base_dir, f'{current_date}_{script_name}_output')
os.makedirs(output_directory, exist_ok=True)

input_copy_path = os.path.join(output_directory, f'{current_date}_{script_name}_input{file_extension}')
copyfile(input_file_path, input_copy_path)

output_file_path = os.path.join(output_directory, f'{current_date}_{script_name}_output{file_extension}')

merge_duplicates(input_file_path, output_file_path)
