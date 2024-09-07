import os
import json
import csv
import pandas as pd
from datetime import datetime
from utilities import connections
from sqlalchemy import text

sql_file_path = os.path.join(os.path.dirname(__file__), '../../../assets/h_statement_report/download_versions.sql')
with open(sql_file_path, 'r') as file:
    download_versions_query = file.read()

connection = connections.get_db_engine('prod')
results_df = pd.read_sql(download_versions_query, connection)

current_date = datetime.now().strftime('%Y-%m-%d')
base_output_directory = os.path.join(os.path.dirname(__file__), '../../../data/h_statement_reports/download_versions', current_date)
os.makedirs(base_output_directory, exist_ok=True)

individual_statements_directory = os.path.join(os.path.dirname(__file__), '../../../data/h_statement_reports/assessor_assigned_statements', current_date, 'individual_statements')
os.makedirs(individual_statements_directory, exist_ok=True)

compiled_statements_directory = os.path.join(os.path.dirname(__file__), '../../../data/h_statement_reports/assessor_assigned_statements', current_date, 'compiled_statements')
os.makedirs(compiled_statements_directory, exist_ok=True)

def extract_statements(file_path):
    statements = []
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        data = json.loads(content)

        if "ghs_h_statements" not in data or data["ghs_h_statements"] is None:
            print(f"Warning: 'ghs_h_statements' not found or None in {file_path}. Skipping file.")
            return statements

        for statement_type, statement_data in data["ghs_h_statements"].items():
            for sub_category_key, sub_category_value in statement_data.items():
                code = sub_category_value.get("code")
                color = sub_category_value.get("color")
                sub_category = sub_category_key.replace("_h_statement", "")
                statements.append((statement_type, sub_category, code, color))

                for nested_key, nested_value in sub_category_value.items():
                    if isinstance(nested_value, dict) and "code" in nested_value:
                        nested_code = nested_value["code"]
                        nested_color = nested_value["color"]
                        nested_sub_category = f"{sub_category}_{nested_key}"
                        statements.append((statement_type, nested_sub_category, nested_code, nested_color))

    return statements

def write_statements_to_csv(statements, csv_file_path):
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Statement Type", "Sub-Category", "Code", "Color"])
        writer.writerows(statements)

def process_directory(input_directory, output_directory):
    for filename in os.listdir(input_directory):
        if filename.endswith(".txt"):
            file_path = os.path.join(input_directory, filename)
            statements = extract_statements(file_path)
            csv_filename = os.path.splitext(filename)[0] + "_h-statements.csv"
            csv_file_path = os.path.join(output_directory, csv_filename)
            write_statements_to_csv(statements, csv_file_path)
            print(f"Processed {filename}")

def process_and_compile_files(input_dir, output_dir):
    columns = ['CF ID', 'CAS Number', 'Name']
    sub_categories = [
        'Skin Corrosion', 'Carcinogenicity', 'Skin Sensitizer', 'Aspiration Hazard',
        'Lactation Toxicity', 'Serious Eye Damage', 'Acute Toxicity Oral',
        'Acute Toxicity Dermal', 'Reproductive Toxicity', 'Germ Cell Mutagenicity',
        'Respiratory Sensitizer', 'Acute Toxicity Inhalation', 'Specific Target Neurotoxicity Single',
        'Specific Target Organ Toxicity Single', 'Specific Target Neurotoxicity Repeated',
        'Specific Target Organ Toxicity Repeated', 'Specific Target Respiratory Toxicity Single',
        'Ozone Layer Hazard', 'Aquatic Hazard Long Term', 'Aquatic Hazard Short Term'
    ]

    sub_category_map = {sub_category.lower().replace(' ', '_'): sub_category for sub_category in sub_categories}
    overall_df = pd.DataFrame(columns=columns + sub_categories)

    for file_name in os.listdir(input_dir):
        if file_name.endswith('.csv'):
            cf_id = file_name.split('_')[0][1:]
            cas_number_parts = file_name.split('_')[1].split('-')
            cas_number = '-'.join(cas_number_parts[:-1]) + '-' + cas_number_parts[-1].split('.')[0].rstrip(')')

            file_path = os.path.join(input_dir, file_name)
            df = pd.read_csv(file_path)

            data = {'CF ID': cf_id, 'CAS Number': cas_number, 'Name': ''}

            for index, row in df.iterrows():
                sub_category = row['Sub-Category']
                code = row['Code']
                color = row['Color']

                if sub_category in sub_category_map:
                    column_name = sub_category_map[sub_category]

                    if pd.notna(code) and pd.notna(color):
                        value = f"{code} - {color}"
                    elif pd.notna(code):
                        value = code
                    elif pd.notna(color):
                        value = color
                    else:
                        value = ''

                    data[column_name] = value

            data_df = pd.DataFrame([data], columns=columns + sub_categories)
            overall_df = pd.concat([overall_df, data_df], ignore_index=True)

    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, 'compiled_data.xlsx')
    overall_df.to_excel(output_file_path, index=False)

for index, row in results_df.iterrows():
    profile_id = row['profile_id']
    cas_rn = row['cas_rn']
    document = row['document']
    date_str = row['inserted_at'].strftime('%Y-%m-%d')
    filename = f"({profile_id}_{cas_rn})_{date_str}_versions.txt"
    filepath = os.path.join(base_output_directory, filename)

    with open(filepath, 'w', encoding='utf-8') as file:
        json_str = json.dumps(document)
        file.write(json_str)

    print(f"Saved document for profile_id {profile_id} and cas_rn {cas_rn} to {filepath}")

process_directory(base_output_directory, individual_statements_directory)
process_and_compile_files(individual_statements_directory, compiled_statements_directory)

print("All tasks completed successfully.")
