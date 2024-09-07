import pandas as pd
import os
import re
from datetime import datetime
import openpyxl
from utilities import connections

def get_code_and_color(term, text):
    if text is None:
        return None
    start_index = text.find(term)
    if start_index == -1:
        return None
    sub_text = text[start_index:]
    if '"code":' in sub_text:
        code_match = re.search(r'H\d{3}', sub_text)
        color_match = re.search(r'"color": "(\w+)"', sub_text)
        if code_match and color_match:
            return f"{code_match.group(0)} - {color_match.group(1)}"
    return None

sql_file_path = os.path.join(os.path.dirname(__file__), '../../../assets/h_statement_report/harmonized_statements.sql')
with open(sql_file_path, 'r') as file:
    query = file.read()

term_to_column = {
    'skin_corrosion_h_statement': 'Skin Corrosion',
    'carcinogenicity_h_statement': 'Carcinogenicity',
    'skin_sensitizer_h_statement': 'Skin Sensitizer',
    'aspiration_hazard_h_statement': 'Aspiration Hazard',
    'lactation_toxicity_h_statement': 'Lactation Toxicity',
    'serious_eye_damage_h_statement': 'Serious Eye Damage',
    'acute_toxicity_oral_h_statement': 'Acute Toxicity Oral',
    'acute_toxicity_dermal_h_statement': 'Acute Toxicity Dermal',
    'reproductive_toxicity_h_statement': 'Reproductive Toxicity',
    'germ_cell_mutagenicity_h_statement': 'Germ Cell Mutagenicity',
    'respiratory_sensitizer_h_statement': 'Respiratory Sensitizer',
    'acute_toxicity_inhalation_h_statement': 'Acute Toxicity Inhalation',
    'specific_target_neurotoxicity_single_h_statement': 'Specific Target Neurotoxicity Single',
    'specific_target_organ_toxicity_single_h_statement': 'Specific Target Organ Toxicity Single',
    'specific_target_neurotoxicity_repeated_h_statement': 'Specific Target Neurotoxicity Repeated',
    'specific_target_organ_toxicity_repeated_h_statement': 'Specific Target Organ Toxicity Repeated',
    'specific_target_respiratory_toxicity_single_h_statement': 'Specific Target Respiratory Toxicity Single',
    'ozone_layer_hazard_h_statement': 'Ozone Layer Hazard',
    'aquatic_hazard_long_term_h_statement': 'Aquatic Hazard Long Term',
    'aquatic_hazard_short_term_h_statement': 'Aquatic Hazard Short Term',
}

results_df = pd.read_sql(query, connections.get_db_engine('prod'))
final_rows = []

for index, row in results_df.iterrows():
    new_row = {
        'pharos_ghs_h_statements': row['pharos_ghs_h_statements'],
        'CF ID': row['profile_id'],
        'CAS Number': row['cas_rn'],
        'Name': row['name'],
    }

    for column in term_to_column.values():
        new_row[column] = None

    if row['pharos_ghs_h_statements'] is not None:
        for term, column in term_to_column.items():
            code_and_color = get_code_and_color(term, row['pharos_ghs_h_statements'])
            if code_and_color:
                new_row[column] = code_and_color

    final_rows.append(new_row)

final_df = pd.DataFrame(final_rows)

output_directory = os.path.join(os.path.dirname(__file__), '../../../data/h_statement_reports/harmonized_statements')
os.makedirs(output_directory, exist_ok=True)

current_date_str = datetime.now().strftime('%Y-%m-%d')

excel_filename = f"harmonized_statements_{current_date_str}.xlsx"
excel_filepath = os.path.join(output_directory, excel_filename)

final_df.to_excel(excel_filepath, index=False, engine='openpyxl')

book = openpyxl.load_workbook(excel_filepath)

for sheetname in book.sheetnames:
    worksheet = book[sheetname]
    cas_col = 'B'
    for row in worksheet[cas_col]:
        row.number_format = '@'

book.save(excel_filepath)

print(f"Saved harmonized statements to {excel_filepath}")
