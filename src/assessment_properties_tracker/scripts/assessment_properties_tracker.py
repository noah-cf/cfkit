import re
import os
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import shutil
from datetime import datetime
from utilities import cas_handling, connections, constants, data_manipulation
import json

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
    
    df['polymer_status'] = df['polymer_parsed'].apply(
        lambda x: x.get('is_polymer') if isinstance(x, dict) else None
    )
    
    df['polymer_molecular_weight'] = df['polymer_parsed'].apply(
        lambda x: x.get('molecular_weight') if isinstance(x, dict) else None
    )
    
    df['polymer_avg_molecular_weight'] = df['polymer_parsed'].apply(
        lambda x: x.get('avg_molecular_weight') if isinstance(x, dict) else None
    )
    
    df['polymer_num_avg_molecular_weight'] = df['polymer_parsed'].apply(
        lambda x: x.get('number_avg_molecular_weight') if isinstance(x, dict) else None
    )
    
    df['polymer_weight_avg_molecular_weight'] = df['polymer_parsed'].apply(
        lambda x: x.get('weight_avg_molecular_weight') if isinstance(x, dict) else None
    )
    
    df['polymer_dispersity'] = df['polymer_parsed'].apply(
        lambda x: x.get('dispersity') if isinstance(x, dict) else None
    )
    
    df['polymer_functional_groups'] = df['polymer_parsed'].apply(
        lambda x: ', '.join(x.get('functional_groups', [])) if isinstance(x, dict) and x.get('functional_groups') else None
    )
    
    df['polymer_monomers'] = df['polymer_parsed'].apply(
        lambda x: ', '.join(x.get('monomers', [])) if isinstance(x, dict) and x.get('monomers') else None
    )
    
    df = df.drop('polymer_parsed', axis=1)

if 'inorganic_chemical' in df.columns:
    df['inorganic_chemical_status'] = df['inorganic_chemical'].apply(
        lambda x: True if isinstance(x, dict) and x.get('is_inorganic') == True 
                 else False if isinstance(x, dict) and x.get('is_inorganic') == False
                 else None if pd.isna(x) 
                 else json.loads(x).get('is_inorganic') if isinstance(x, str) and x 
                 else None
    )

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

cas_column_index = df.columns.get_loc('cas_rn')

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
    },
    {
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "COLUMNS",
                "startIndex": df.columns.get_loc('name'),
                "endIndex": df.columns.get_loc('name') + 1
            },
            "properties": {
                "pixelSize": 200
            },
            "fields": "pixelSize"
        }
    }
]

spreadsheet.batch_update({"requests": requests})

worksheet.format("A1:AU1000", {"wrapStrategy": "CLIP"})

current_date = datetime.now().strftime('%Y_%m_%d')

output_directory = os.path.join(os.path.dirname(__file__), f'../../../data/assessment_properties_tracker/{current_date}_output')
os.makedirs(output_directory, exist_ok=True)

excel_output_file = os.path.join(output_directory, f'{current_date}_assessment_properties_tracker_output.xlsx')
df.to_excel(excel_output_file, index=False)

shutil.copy(sql_file_path, os.path.join(output_directory, f'{current_date}_assessment_properties_tracker_query.sql'))

print(f"Data has been saved to Google Sheets and also exported to Excel at {excel_output_file}")