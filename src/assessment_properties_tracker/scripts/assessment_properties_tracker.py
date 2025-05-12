import re
import os
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import shutil
from datetime import datetime
from utilities import cas_handling, connections, data_manipulation
import json
import numpy as np

sql_file_path = os.path.join(os.path.dirname(__file__), '../../../assets/assessment_properties_tracker/assessment_properties_tracker.sql')
with open(sql_file_path, 'r') as file:
    tracker_properties_query = file.read()

engine = connections.get_db_engine('prod')

df = pd.read_sql(tracker_properties_query, engine)

df = data_manipulation.datetime_delocal_date_only(df)

if 'polymer' in df.columns:
    def parse_polymer_json(x):
        if pd.isna(x):
            return None
        if isinstance(x, str) and x:
            try:
                return json.loads(x)
            except json.JSONDecodeError:
                return None
        return x if isinstance(x, dict) else None
    
    df['polymer_parsed'] = df['polymer'].apply(parse_polymer_json)
    
    all_polymer_keys = set()
    for idx, parsed in df['polymer_parsed'].items():
        if isinstance(parsed, dict):
            all_polymer_keys.update(parsed.keys())
    
    for key in sorted(all_polymer_keys):
        if key == 'functional_groups' or key == 'monomers':
            df[f'polymer_{key}'] = df['polymer_parsed'].apply(
                lambda x: ', '.join(x.get(key, [])) if isinstance(x, dict) and x.get(key) else None
            )
        else:
            df[f'polymer_{key}'] = df['polymer_parsed'].apply(
                lambda x: x.get(key) if isinstance(x, dict) else None
            )
    
    df = df.drop('polymer_parsed', axis=1)

if 'inorganic_chemical' in df.columns:
    def parse_inorganic_json(x):
        if pd.isna(x):
            return None
        if isinstance(x, str) and x:
            try:
                return json.loads(x)
            except json.JSONDecodeError:
                return None
        return x if isinstance(x, dict) else None
    
    df['inorganic_parsed'] = df['inorganic_chemical'].apply(parse_inorganic_json)
    
    all_inorganic_keys = set()
    for idx, parsed in df['inorganic_parsed'].items():
        if isinstance(parsed, dict):
            all_inorganic_keys.update(parsed.keys())
    
    print(f"Found {len(all_inorganic_keys)} unique inorganic chemical fields")
    
    for key in sorted(all_inorganic_keys):
        column_name = f'inorganic_{key}'
        
        def extract_field(x, field_key=key):
            if not isinstance(x, dict):
                return None
            value = x.get(field_key)
            
            if field_key in ['maximum_diameter', 'minimum_diameter'] and isinstance(value, str):
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return value
            
            return value
        
        df[column_name] = df['inorganic_parsed'].apply(extract_field)
    
    df = df.drop('inorganic_parsed', axis=1)

if 'botanical' in df.columns:
    def parse_botanical_json(x):
        if pd.isna(x):
            return None
        if isinstance(x, str) and x:
            try:
                return json.loads(x)
            except json.JSONDecodeError:
                return None
        return x if isinstance(x, dict) else None
    
    df['botanical_parsed'] = df['botanical'].apply(parse_botanical_json)
    
    all_botanical_keys = set()
    for idx, parsed in df['botanical_parsed'].items():
        if isinstance(parsed, dict):
            all_botanical_keys.update(parsed.keys())
    
    print(f"Found {len(all_botanical_keys)} unique botanical fields")
    
    for key in sorted(all_botanical_keys):
        column_name = f'botanical_{key}'
        df[column_name] = df['botanical_parsed'].apply(
            lambda x: x.get(key) if isinstance(x, dict) else None
        )
    
    df = df.drop('botanical_parsed', axis=1)

if 'additional_chemical_characterization' in df.columns:
    df = cas_handling.transform_cf_additional_casrns(df)

df = data_manipulation.clear_false(df)

df['cas_rn'] = df['cas_rn'].fillna("").astype(str)
df['cas_rn'] = df['cas_rn'].apply(lambda x: re.sub('[^0-9-]', '', x.strip().replace('\n', '')))

df["ChemFORWARD Link"] = "https://alternatives.chemforward.org/app/profiles/assessment/" + df['id'].astype(str) + "/summary"

scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(connections.Config.GOOGLE_CLOUD_KEY, scope)
client = gspread.authorize(creds)

spreadsheet_name = 'CHA Properties Reports'
spreadsheet = client.open(spreadsheet_name)

current_date = datetime.now().strftime('%Y-%m-%d')
dynamic_name = f"({current_date}) CHA Properties"
worksheet = spreadsheet.add_worksheet(title=dynamic_name, rows="100", cols="20")

if 'cas_rn' in df.columns:
    cas_column_index = df.columns.get_loc('cas_rn')
else:
    cas_column_index = 0

format_request = {
    "repeatCell": {
        "range": {
            "sheetId": worksheet.id,
            "startRowIndex": 0,
            "endRowIndex": len(df),
            "startColumnIndex": cas_column_index,
            "endColumnIndex": cas_column_index + 1
        },
        "cell": {
            "userEnteredFormat": {
                "numberFormat": {
                    "type": "TEXT"
                }
            }
        },
        "fields": "userEnteredFormat.numberFormat"
    }
}

spreadsheet.batch_update({"requests": [format_request]})
set_with_dataframe(worksheet, df)

requests = [
    {
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "ROWS",
                "startIndex": 0,
                "endIndex": 1
            },
            "properties": {
                "pixelSize": 60
            },
            "fields": "pixelSize"
        }
    },
    {
        "repeatCell": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": 0,
                "endRowIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "bold": True
                    },
                    "verticalAlignment": "MIDDLE"
                }
            },
            "fields": "userEnteredFormat.textFormat.bold, userEnteredFormat.verticalAlignment"
        }
    },
    {
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "ROWS",
                "startIndex": 1
            },
            "properties": {
                "pixelSize": 45
            },
            "fields": "pixelSize"
        }
    },
    {
        "repeatCell": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "verticalAlignment": "MIDDLE"
                }
            },
            "fields": "userEnteredFormat.verticalAlignment"
        }
    },
    {
        "updateSheetProperties": {
            "properties": {
                "sheetId": worksheet.id,
                "gridProperties": {
                    "frozenRowCount": 1
                }
            },
            "fields": "gridProperties.frozenRowCount"
        }
    },
    {
        'updateSheetProperties': {
            'properties': {
                'sheetId': worksheet.id,
                'index': 0
            },
            'fields': 'index'
        }
    }
]

if 'name' in df.columns:
    name_column_index = df.columns.get_loc('name')
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "COLUMNS",
                "startIndex": name_column_index,
                "endIndex": name_column_index + 1
            },
            "properties": {
                "pixelSize": 200
            },
            "fields": "pixelSize"
        }
    })

spreadsheet.batch_update({"requests": requests})
worksheet.format("A1:ZZ1000", {"wrapStrategy": "CLIP"})

current_date = datetime.now().strftime('%Y_%m_%d')
output_directory = os.path.join(os.path.dirname(__file__), f'../../../data/assessment_properties_tracker/{current_date}_output')
os.makedirs(output_directory, exist_ok=True)

excel_output_file = os.path.join(output_directory, f'{current_date}_assessment_properties_tracker_output.xlsx')
df.to_excel(excel_output_file, index=False)

shutil.copy(sql_file_path, os.path.join(output_directory, f'{current_date}_assessment_properties_tracker_query.sql'))

print(f"Data has been saved to Google Sheets and also exported to Excel at {excel_output_file}")
print(f"Total columns in final dataset: {len(df.columns)}")
print(f"Sample of new inorganic columns: {[col for col in df.columns if col.startswith('inorganic_')][:10]}")