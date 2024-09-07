from datetime import datetime
import os
import pandas as pd
from sqlalchemy import text
from utilities import connections
from gspread_dataframe import set_with_dataframe
from gspread_formatting import cellFormat, textFormat, format_cell_range, set_column_widths
import shutil

sql_file_path = os.path.join(os.path.dirname(__file__), '../../../assets/user_report/user_report.sql')
with open(sql_file_path, 'r') as file:
    user_report_query = file.read()

engine = connections.get_db_engine('prod')
with engine.connect() as connection:
    result = connection.execute(text(user_report_query))
    df = pd.DataFrame(result.fetchall(), columns=result.keys())

df['Last Sign In'] = df['last_signed_in_at'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '')
df['Account Made'] = df['inserted_at'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '')
df['Industry'] = df['industry_id'].map({
    1: 'BPC',
    2: 'Electronics',
    3: 'Fashion & Apparel',
    4: 'Food Packaging',
    5: 'Google CHAs',
    6: 'Plastics',
    7: 'DMF Portfolio',
    11: 'Solvents/LVOC',
    26: 'All CHAs',
    12: 'Locked',
    25: 'Google CHAs'
})
df = df[['name', 'company', 'email', 'Last Sign In', 'Account Made', 'Industry']]

industry_counts = df['Industry'].value_counts().reset_index()
industry_counts.columns = ['Industry', 'Count']

client = connections.get_google_sheets_client()
spreadsheet_name = 'User Reports'
spreadsheet = client.open(spreadsheet_name)
current_date = datetime.now().strftime('%Y-%m-%d')
worksheet = spreadsheet.add_worksheet(title=current_date, rows="100", cols="20")

set_with_dataframe(worksheet, df)
set_with_dataframe(worksheet, industry_counts, row=2, col=len(df.columns) + 2)

format_cell_range(worksheet, 'A1:Z1000', cellFormat(textFormat=textFormat(fontSize=12)))
format_cell_range(worksheet, 'A1:F1', cellFormat(textFormat=textFormat(bold=True, fontSize=12)))
set_column_widths(worksheet, [
    ('A', 300),
    ('B', 300),
    ('C', 300),
    ('D', 200),
    ('E', 200),
    ('F', 200)
])
format_cell_range(worksheet, 'D1:F1000', cellFormat(horizontalAlignment='CENTER'))

spreadsheet.batch_update({
    'requests': [
        {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': worksheet.id,
                    'index': 1
                },
                'fields': 'index'
            }
        }
    ]
})

current_date_folder = datetime.now().strftime('%Y_%m_%d')
script_name = 'user_report'

base_dir = os.path.join(os.path.dirname(__file__), f'../../../data/user_reports/{current_date_folder}_{script_name}_output')
os.makedirs(base_dir, exist_ok=True)

output_file_path = os.path.join(base_dir, f'{current_date_folder}_{script_name}_output.xlsx')
df.to_excel(output_file_path, index=False)

shutil.copy(sql_file_path, os.path.join(base_dir, f'{current_date_folder}_{script_name}_query.sql'))

print(f"User report saved to {output_file_path}")
print("Script executed successfully.")
