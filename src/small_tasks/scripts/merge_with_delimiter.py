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

    column_index = ord(column_to_merge) - ord('A')
    if column_index < 0 or column_index >= len(df.columns):
        raise ValueError(f"Invalid column letter: {column_to_merge}. Valid range is A-{chr(ord('A') + len(df.columns) - 1)}")
    
    column_name = df.columns[column_index]
    
    columns_with_data = []
    for col in df.columns:
        col_has_data = False
        for val in df[col]:
            if pd.notna(val) and str(val).strip() != '':
                col_has_data = True
                break
        if col_has_data:
            columns_with_data.append(col)
    
    if len(columns_with_data) == 1 and columns_with_data[0] == column_name:
        txt_output_file = os.path.splitext(output_file)[0] + '.txt'
        
        values = [str(val) for val in df[column_name] if pd.notna(val) and str(val).strip() != '']
        result_string = delimiter.join(values)
        
        with open(txt_output_file, 'w') as f:
            f.write(result_string)
        
        print(f"Only one column with data detected. Values have been joined and saved to {txt_output_file}")
        print(f"Number of values joined: {len(values)}")
        print(f"Note: Output saved as plain text (.txt) to avoid file format limitations.")
    else:
        group_columns = [col for col in df.columns if col != column_name]
        df_merged = df.groupby(group_columns, as_index=False).agg(
            lambda x: delimiter.join([str(v) for v in x if pd.notna(v) and str(v).strip() != '']) 
            if x.name == column_name else x.iloc[0]
        )

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