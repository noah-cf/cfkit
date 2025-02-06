import os
import pandas as pd
from shutil import copyfile
from datetime import datetime

def split_column_by_delimiter(input_file, output_file):
    column_to_split = input("Which column has the items to split? (Use column letter, e.g., 'A'): ").upper()
    delimiter = input("What is the delimiter? (Use '\\n' for a new line): ")
    if delimiter == "\\n":
        delimiter = "\n"

    file_extension = os.path.splitext(input_file)[1].lower()
    if file_extension == '.xlsx':
        df = pd.read_excel(input_file)
    elif file_extension == '.csv':
        df = pd.read_csv(input_file)
    else:
        raise ValueError(f"Unsupported file type: {file_extension}. Only .xlsx and .csv are supported.")

    column_name = df.columns[ord(column_to_split) - ord('A')]
    split_data = df[column_name].str.split(delimiter, expand=True).stack().reset_index(level=1, drop=True)
    df = df.drop(columns=[column_name]).join(split_data.rename(column_name))

    if file_extension == '.xlsx':
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            sheet_base_name = "Sheet"
            max_rows = 1_000_000
            for i, start in enumerate(range(0, len(df), max_rows)):
                sheet_name = f"{sheet_base_name}_{i}"
                df.iloc[start:start + max_rows].to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Data has been split into multiple sheets and saved to {output_file}")

    elif file_extension == '.csv':
        df.to_csv(output_file, index=False)
    
    print(f"Data has been split and saved to {output_file}")

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/small_tasks/split_by_delimiter"))
input_files = [f for f in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, f))]
supported_files = [f for f in input_files if f.endswith(('.xlsx', '.csv'))]
if not supported_files:
    raise FileNotFoundError(f"No valid .xlsx or .csv input file found in {base_dir}")

input_file_name = supported_files[0]
input_file_path = os.path.join(base_dir, input_file_name)

file_extension = os.path.splitext(input_file_name)[1].lower()

current_date = datetime.now().strftime('%Y_%m_%d')

script_name = 'split_by_delimiter'

output_directory = os.path.join(base_dir, f'{current_date}_{script_name}_output')
os.makedirs(output_directory, exist_ok=True)

input_copy_path = os.path.join(output_directory, f'{current_date}_{script_name}_input{file_extension}')
copyfile(input_file_path, input_copy_path)

output_file_path = os.path.join(output_directory, f'{current_date}_{script_name}_output{file_extension}')

split_column_by_delimiter(input_file_path, output_file_path)
