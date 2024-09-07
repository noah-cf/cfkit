import json
import openpyxl
import pandas as pd
import sqlalchemy
import os

import utilities.connections as connections

script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/03_fetch_data_cf_input.xlsx')
output_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/03_fetch_data_cf_output.xlsx')
cas_column = 'Final CAS'

sql_file_path = os.path.join(os.path.dirname(__file__), '../../../assets/ingredient_intelligence_report/03_fetch_data_cf.sql')
with open(sql_file_path, 'r') as file:
    REPORT_INGREDIENT_CAS = file.read()

def get_data(conn, query):
    return pd.read_sql(query, conn)


def gather_data(file_path, cas_column):
    dtype = {cas_column: str}

    if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
        data = pd.read_excel(file_path, dtype=dtype)
    elif file_path.endswith('.csv'):
        data = pd.read_csv(file_path, dtype=dtype)
    else:
        raise ValueError("Invalid file format. Please provide an Excel (.xlsx, .xls) or CSV (.csv) file.")

    conn = connections.get_db_engine('prod')
    db_data = get_data(conn, REPORT_INGREDIENT_CAS)

    db_data['additional_casrns'] = db_data['additional_casrns'].apply(
        lambda x: ', '.join(i['additional_casrn'] for i in json.loads(x)) if isinstance(x, str) and x.strip() != '[]' else x)
    db_data.columns = ['cf_' + col for col in db_data.columns]

    data = pd.merge(data, db_data, how='left', left_on=cas_column, right_on='cf_cas_rn')

    data['CF Data'] = data['cf_cas_rn'].apply(lambda x: 'no_cf_data' if pd.isnull(x) else 'cf_data')

    return data


result = gather_data(input_file, cas_column)

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    result.to_excel(writer, index=False, sheet_name='Sheet1')

wb = openpyxl.load_workbook(output_file)
ws = wb.active
for row in ws.iter_rows(min_row=2, max_row=len(result) + 1):
    cas_cell = row[0]
    cas_cell.number_format = '@'

wb.save(output_file)
