from datetime import datetime
import re
import os
import gspread
import numpy as np
import pandas as pd
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import shutil

from utilities import connections, constants, cas_handling, data_manipulation

sql_file_path = os.path.join(os.path.dirname(__file__), '../../../assets/assessment_tracker/assessment_tracker.sql')
with open(sql_file_path, 'r') as file:
    tracker_assessment_query = file.read()
engine = connections.get_db_engine('prod')  # Replace 'prod' with 'stg' if needed

df = connections.run_query(engine, tracker_assessment_query)
df = data_manipulation.datetime_delocal_date_only(df)

custom_order = [
    99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 843, 844, 70, 47, 63,
    64, 62, 134, 135, 139, 138, 126, 132, 133, 131, 130, 129, 125, 122, 128,
    24, 26, 34, 31, 28, 27, 29, 30, 33, 32, 110, 111, 112, 113, 566, 40, 41,
    42, 43, 45, 44, 48, 49, 50, 51, 57, 52, 56, 53, 54, 55, 562, 74, 79, 80,
    81, 82, 89, 75, 76, 77, 78, 73, 67, 68, 85, 71, 86, 118, 567, 127, 163,
    162, 161, 160, 159, 170, 169, 166, 165, 164, 172, 173, 563, 171, 174, 175,
    564, 565, 845, 890, 902, 891, 901, 910, 395, 1395, 865, 1382, 872, 861,
    869, 870, 863, 848, 864, 964, 965, 1398, 1399, 938, 2052, 535, 1876, 1877,
    1878, 1894, 547, 777, 1899, 2053, 390, 2054, 1901, 1902, 1903, 1904, 1937,
    548, 1906, 1907, 1412, 1413, 1414, 1415, 1425, 1426, 1427, 1428, 1433,
    1419, 1422, 1431, 1432, 2031, 2044, 2043, 2039, 205, 2049, 1031, 1147,
    1040, 2050, 1124, 1099, 1078, 1106, 1087, 981, 988, 1136, 1028, 1016,
    2047, 2046, 1785, 1140, 1076, 1131, 1064, 1474, 1193, 1084, 984, 1161,
    1405, 1938, 2502, 2501, 2534, 2522, 2521, 2518, 2515, 2513, 2516, 2517,
    2504, 2403, 2520, 2514, 1212, 1954, 1113, 1358, 3414, 1974, 3423, 3422,
    3453, 3474, 3480, 2512, 1209, 3483, 3482, 3481, 3486, 3509, 3510, 2187,
    3515, 3516, 3514, 3508, 937, 556, 3492, 942, 3494, 1488, 3496, 3497, 3343,
    3356, 944, 4030, 588, 950, 1027, 4034, 3685, 3737, 1811, 3983, 4066, 1603,
    3499, 2925, 3501, 3503, 3504, 3505, 3506, 3507, 3511, 3518, 3519, 1668,
    943, 953, 3888, 3994, 658, 3249, 3297, 3512, 3995, 955, 3370, 3935, 3560,
    1330, 2865, 1434, 1506, 1532, 859, 1331, 1536, 1538, 4102, 1077, 4062,
    4061, 2508, 3446, 3673, 867, 871, 1299, 2532, 561, 868, 947, 954, 4077,
    3664, 4094, 4091, 4095, 4092, 2019, 3999, 4000, 1377, 546, 558, 3335,
    3607, 1958, 545, 550, 4099, 4100, 3978, 1169
]

df_custom_order = df[df['id'].isin(custom_order)].copy()
df_remaining = df[~df['id'].isin(custom_order)]

order_mapping = {custom_order_id: index for index, custom_order_id in enumerate(custom_order)}
df_custom_order['order_index'] = df_custom_order['id'].map(order_mapping)
df_custom_order = df_custom_order.sort_values('order_index')
df_custom_order.drop('order_index', axis=1, inplace=True)

print(df_remaining.columns)

df_remaining_sorted = df_remaining.sort_values(['sponsor', 'Draft Date'])

df_final = pd.concat([df_custom_order, df_remaining_sorted], ignore_index=True)
df = df_final

df = cas_handling.transform_cf_additional_casrns(df)
df = data_manipulation.clear_false(df)

for col in ['cas_rn']:
    df[col] = df[col].fillna("").astype(str)
    df[col] = df[col].apply(lambda x: re.sub(r'[^0-9\-]', '', x.strip().replace('\n', '')))


df["ChemFORWARD Link"] = "https://alternatives.chemforward.org/app/profiles/assessment/" + df['id'].astype(str) + "/summary"

df["Tagging Book"] = "https://docs.google.com/spreadsheets/d/1AaXhiDiTwJTayBunpOdGXxtfVw3Q7BM8DOX2Lyb7ts8/edit?usp=sharing"

df = data_manipulation.map_values(df, "status", constants.Dictionaries.cf_status)
df = data_manipulation.map_values(df, "manual_hazard_band_score", constants.Dictionaries.manual_hazard_band_score)
df = data_manipulation.map_column_names(df, constants.Dictionaries.cf_columns)

scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(connections.Config.GOOGLE_CLOUD_KEY, scope)
client = gspread.authorize(creds)

spreadsheet_name = 'CHA Reports (Stg)'
spreadsheet = client.open(spreadsheet_name)

current_date = datetime.now().strftime('%Y-%m-%d')

all_worksheets = spreadsheet.worksheets()
previous_worksheet = None
previous_date = datetime.strptime('1900-01-01', '%Y-%m-%d').date()

for worksheet in all_worksheets:
    if 'CHAs' in worksheet.title:
        worksheet_date_str = worksheet.title.split('(')[-1].split(')')[0].strip()
        worksheet_date = datetime.strptime(worksheet_date_str, '%Y-%m-%d').date()
        if worksheet_date > previous_date:
            previous_worksheet = worksheet
            previous_date = worksheet_date

if previous_worksheet is not None:
    previous_data = pd.DataFrame(previous_worksheet.get_all_records())
    if not previous_data.empty:
        current_data = pd.merge(df, previous_data[
            ['CF ID',
             'Comments (User Entered)',
             'Initial CHA Deadline (User Entered)',
             'Verifier (User Entered)',
             'Scoring Workbook (User Entered)',
             'Provisional C2C Rating (User Entered)',
             'Provisional Hazard Band (User Entered)',
             'Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)',
             'Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)'
        ]], on='CF ID', how='left')
        
        current_data['Comments (User Entered)'] = current_data[
            'Comments (User Entered)_y'].fillna(
            current_data['Comments (User Entered)_x'])
        
        current_data['Initial CHA Deadline (User Entered)'] = current_data[
            'Initial CHA Deadline (User Entered)_y'].fillna(
            current_data['Initial CHA Deadline (User Entered)_x'])
        
        current_data['Verifier (User Entered)'] = current_data[
            'Verifier (User Entered)_y'].fillna(
            current_data['Verifier (User Entered)_x'])
        
        current_data['Scoring Workbook (User Entered)'] = current_data[
            'Scoring Workbook (User Entered)_y'].fillna(
            current_data['Scoring Workbook (User Entered)_x'])
        
        current_data['Provisional C2C Rating (User Entered)'] = current_data[
            'Provisional C2C Rating (User Entered)_y'].fillna(
            current_data['Provisional C2C Rating (User Entered)_x'])
        
        current_data['Provisional Hazard Band (User Entered)'] = current_data[
            'Provisional Hazard Band (User Entered)_y'].fillna(
            current_data['Provisional Hazard Band (User Entered)_x'])
        
        current_data['Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)'] = current_data[
            'Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)_y'].fillna(
                current_data['Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)_x'])
        
        current_data['Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)'] = current_data[
            'Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)_y'].fillna(
                current_data['Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)_x'])
        
        current_data = current_data.drop(
            ['Comments (User Entered)_x',
             'Comments (User Entered)_y',
             'Initial CHA Deadline (User Entered)_x',
             'Initial CHA Deadline (User Entered)_y',
             'Scoring Workbook (User Entered)_x',
             'Scoring Workbook (User Entered)_y',
             'Verifier (User Entered)_x',
             'Verifier (User Entered)_y',
             'Provisional C2C Rating (User Entered)_x',
             'Provisional C2C Rating (User Entered)_y',
             'Provisional Hazard Band (User Entered)_x',
             'Provisional Hazard Band (User Entered)_y',
             'Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)_x',
             'Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)_y',
             'Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)_x',
             'Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)_y'
        ], axis=1)
        
        column_order = [
            'CF ID',
            'Direct Sponsorship',
            'Sponsor',
            'Project',
            'Verticals',
            'Tagging Book',
            'ChemFORWARD Link',
            'CAS Number',
            'Additional CAS',
            'EC Number',
            'Chemical Name',
            'INCI',
            'Free CHA?',
            'C2C Score',
            'Hazard Band',
            'Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)',
            'Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)',
            'Scoring Workbook (User Entered)',
            'Assessor Group',
            'Initial CHA Deadline (User Entered)',
            'Status',
            'Provisional?',
            'Provisional Date',
            'Provisional C2C Rating (User Entered)',
            'Provisional Hazard Band (User Entered)',
            'Verifier (User Entered)',
            'Database Verifier',
            'Comments (User Entered)',
            'Draft Date',
            'Submitted Date',
            'In Review Date',
            'Assessor Assignment Date',
            'Last Assessor Date',
            'Verifier Assignment Date',
            'Last Verifier Date',
            'First Verified Date',
            'Last Verified Date'
        ]
        df = current_data[column_order]

df.insert(df.columns.get_loc("Provisional Hazard Band (User Entered)") + 1, 'Verification Length',
          df['First Verified Date'] - df['Submitted Date'])
df.insert(df.columns.get_loc("Verification Length") + 1, 'Initial Draft Length',
          df['Submitted Date'] - df['Draft Date'])

dynamic_name = f"({current_date}) CHAs"
worksheet = spreadsheet.add_worksheet(title=dynamic_name, rows="100", cols="20")

requests = [
    {
        'updateSheetProperties': {
            'properties': {
                'sheetId': worksheet.id,
                'index': 0,
            },
            'fields': 'index',
        }
    }
]
spreadsheet.batch_update({'requests': requests})

format_request = {
    "repeatCell": {
        "range": {
            "sheetId": worksheet.id,
            "startRowIndex": 0,
            "endRowIndex": len(df),
            "startColumnIndex": 6,  # Column G
            "endColumnIndex": 8  # Column I
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

# Define the dynamic part of the folder name
folder_name = 'assessment_tracker'  # You can adjust this as per your needs

# Get the current date in the format YYYY_MM_DD
current_date_folder = datetime.now().strftime('%Y_%m_%d')

# Create the output folder with the current date and folder name
output_directory = os.path.join(os.path.dirname(__file__), f'../../../data/assessment_tracker/{current_date_folder}_{folder_name}_output')
os.makedirs(output_directory, exist_ok=True)

# Save the DataFrame to an Excel file in the output directory, with the custom name
excel_output_file = os.path.join(output_directory, f'{current_date_folder}_{folder_name}_output.xlsx')
df.to_excel(excel_output_file, index=False)

# Copy the input SQL file or other relevant files to the output directory, with the custom name
shutil.copy(sql_file_path, os.path.join(output_directory, f'{current_date_folder}_{folder_name}_query.sql'))

print(f"Data has been saved to Google Sheets and also exported to Excel at {excel_output_file}")

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
    }
]

deadline_index = df.columns.get_loc("Initial CHA Deadline (User Entered)")
verifier_corr_index = df.columns.get_loc("Verifier (User Entered)")
comments_index = df.columns.get_loc("Comments (User Entered)")
scoring_book_index = df.columns.get_loc("Scoring Workbook (User Entered)")
provisional_c2c_user_index = df.columns.get_loc("Provisional C2C Rating (User Entered)")
provisional_hazard_band_user_index = df.columns.get_loc("Provisional Hazard Band (User Entered)")

columns_to_format = [deadline_index,
                     verifier_corr_index,
                     comments_index,
                     scoring_book_index,
                     provisional_c2c_user_index,
                     provisional_hazard_band_user_index]

for col_index in columns_to_format:
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": len(df),
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1
                }],
                "booleanRule": {
                    "condition": {
                        "type": "NOT_BLANK"
                    },
                    "format": {
                        "backgroundColor": {
                            "red": 1,
                            "green": 0.8,
                            "blue": 0.8
                        }
                    }
                }
            },
            "index": 0
        }
    })

draft_date_index = df.columns.get_loc("Draft Date")
last_verified_date_index = df.columns.get_loc("Last Verified Date")

requests.append({
    "addConditionalFormatRule": {
        "rule": {
            "ranges": [{
                "sheetId": worksheet.id,
                "startRowIndex": 1,
                "endRowIndex": len(df) + 1,
                "startColumnIndex": draft_date_index,
                "endColumnIndex": last_verified_date_index + 1
            }],
            "booleanRule": {
                "condition": {
                    "type": "NOT_BLANK"
                },
                "format": {
                    "backgroundColor": {
                        "red": 1,
                        "green": 1,
                        "blue": 0.6
                    }
                }
            }
        },
        "index": 1
    }
})

requests.append({
    "repeatCell": {
        "range": {
            "sheetId": worksheet.id,
            "startRowIndex": 0,
        },
        "cell": {
            "userEnteredFormat": {
                "wrapStrategy": "WRAP"
            }
        },
        "fields": "userEnteredFormat.wrapStrategy"
    }
})

columns_to_center = [
    df.columns.get_loc('CF ID'),
    df.columns.get_loc('CAS Number'),
    df.columns.get_loc('EC Number'),
    df.columns.get_loc('C2C Score'),
    df.columns.get_loc('Hazard Band'),
    df.columns.get_loc('Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)'),
    df.columns.get_loc('Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)'),
    df.columns.get_loc('Status'),
    df.columns.get_loc('Provisional C2C Rating (User Entered)'),
    df.columns.get_loc('Provisional Hazard Band (User Entered)'),
    df.columns.get_loc('Provisional Date'),
    df.columns.get_loc('Verification Length'),
    df.columns.get_loc('Initial Draft Length')
]

for col_index in columns_to_center:
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": worksheet.id,
                "startColumnIndex": col_index,
                "endColumnIndex": col_index + 1
            },
            "cell": {
                "userEnteredFormat": {
                    "horizontalAlignment": "CENTER"
                }
            },
            "fields": "userEnteredFormat.horizontalAlignment"
        }
    })

columns_double_width = [
    df.columns.get_loc('Project'),
    df.columns.get_loc('Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)'),
    df.columns.get_loc('Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)'),
    df.columns.get_loc('Scoring Workbook (User Entered)'),
    df.columns.get_loc('Assessor Group'),
    df.columns.get_loc('Initial CHA Deadline (User Entered)'),
    df.columns.get_loc('Status'),
    df.columns.get_loc('Verification Length'),
    df.columns.get_loc('Initial Draft Length'),
    df.columns.get_loc('Verifier (User Entered)'),
    df.columns.get_loc('Draft Date'),
    df.columns.get_loc('Submitted Date'),
    df.columns.get_loc('In Review Date'),
    df.columns.get_loc('Assessor Assignment Date'),
    df.columns.get_loc('Last Assessor Date'),
    df.columns.get_loc('Verifier Assignment Date'),
    df.columns.get_loc('Last Verifier Date'),
    df.columns.get_loc('First Verified Date'),
    df.columns.get_loc('Last Verified Date'),
]

columns_triple_width = [
    df.columns.get_loc('Chemical Name'),
    df.columns.get_loc('Comments (User Entered)'),
]

for col_index in columns_double_width:
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "COLUMNS",
                "startIndex": col_index,
                "endIndex": col_index + 1
            },
            "properties": {
                "pixelSize": 200
            },
            "fields": "pixelSize"
        }
    })

for col_index in columns_triple_width:
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "COLUMNS",
                "startIndex": col_index,
                "endIndex": col_index + 1
            },
            "properties": {
                "pixelSize": 300
            },
            "fields": "pixelSize"
        }
    })

requests.append({
    "updateSheetProperties": {
        "properties": {
            "sheetId": worksheet.id,
            "gridProperties": {
                "frozenRowCount": 1
            }
        },
        "fields": "gridProperties.frozenRowCount"
    }
})

current_status_index = df.columns.get_loc('Status')
verifier_correction_index = df.columns.get_loc('Verifier (User Entered)')

for col_index in [current_status_index, verifier_correction_index]:
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": worksheet.id,
                "startColumnIndex": col_index,
                "endColumnIndex": col_index + 1
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "bold": True
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat.bold"
        }
    })

requests.append({
    "addConditionalFormatRule": {
        "rule": {
            "ranges": [{
                "sheetId": worksheet.id,
                "startRowIndex": 1,
                "endRowIndex": len(df),
                "startColumnIndex": current_status_index,
                "endColumnIndex": current_status_index + 1
            }],
            "booleanRule": {
                "condition": {
                    "type": "TEXT_CONTAINS",
                    "values": [{"userEnteredValue": "IP - "}]
                },
                "format": {
                    "backgroundColor": {
                        "red": 0.9,
                        "green": 0.7,
                        "blue": 0.9
                    }
                }
            }
        },
        "index": 0
    }
})

requests.append({
    "addConditionalFormatRule": {
        "rule": {
            "ranges": [{
                "sheetId": worksheet.id,
                "startRowIndex": 1,
                "endRowIndex": len(df),
                "startColumnIndex": current_status_index,
                "endColumnIndex": current_status_index + 1
            }],
            "booleanRule": {
                "condition": {
                    "type": "TEXT_CONTAINS",
                    "values": [{"userEnteredValue": "Full CHA"}]
                },
                "format": {
                    "backgroundColor": {
                        "red": 0.8,
                        "green": 1,
                        "blue": 0.8
                    }
                }
            }
        },
        "index": 1
    }
})


spreadsheet.batch_update({"requests": requests})

hover_tips = {
    'CF ID': "Chemical ID within the App",
    'Direct Sponsorship': "True/False based on if the CHA was a direct sponsorship",
    'Sponsor': "Sponsor as entered within the Admin portal for the respective chemical",
    'Project': "Project as entered within the Admin portal for the respective chemical",
    'Verticals': "Verticals for which the respective Chemical has any entered Taxonomy",
    'Tagging Book': "Link to the tagging workbook (TBC)",
    'ChemFORWARD Link': "Link to the App page for the respective chemical",
    'CAS Number': "Respective Chemical CAS Number",
    'Additional CAS': "In-app Additional CAS Numbers (Not Pharos)",
    'EC Number': "In-app EC Numbers",
    'Chemical Name': "In-app Chemical Name",
    'INCI': "In-app INCI Names",
    'Free CHA?': "Is this CHA marked as Free in the App?",
    'C2C Score': "In-app C2C Rating",
    'Hazard Band': "In-app Hazard Band",
    'Chemical Rating Guidance v1.1Rev C2CC Score (User Entered)': "Maintains C2C Score for the purpose of integrity during scoring updates",
    'Chemical Rating Guidance v1.1Rev Hazard Band (User Entered)': "Maintains Hazard Band for the purpose of integrity during scoring updates",
    'Scoring Workbook (User Entered)': "Respective Chemical scoring workbook link",
    'Assessor Group': "Respective Chemical in-App Assessor Group",
    'Initial CHA Deadline (User Entered)': "User Entered CHA Deadline",
    'Status': "Respective Chemical in-App Status",
    'Provisional?': "Is the respective chemical marked as provisional within the app?",
    'Provisional Date': "Date the chemical was marked as provisional",
    'Provisional C2C Rating (User Entered)': "Provisional C2C Rating entered by the user (Not necessarily in-App)",
    'Provisional Hazard Band (User Entered)': "Provisional hazard band entered by the user (Not necessarily in-App)",
    'Verification Length': "Time taken for verification. Calculated as difference between Verifier Assignment & Submitted",
    'Initial Draft Length': "Time taken for verification. Calculated as difference between Submitted date and Draft date.",
    'Verifier (User Entered)': "User entered verifier for the chemical (Not in-App)",
    'Database Verifier': "In-App Verifier records",
    'Comments (User Entered)': "User entered comments for the chemical",
    'Draft Date': "Date the CHA became a Draft",
    'Submitted Date': "Date the CHA was Submitted",
    'In Review Date': "Date the chemical was set to 'In Review'",
    'Assessor Assignment Date': "Date the chemical was first Assigned to an Assessor",
    'Last Assessor Date': "Most recent date the chemical was Assigned to an Assessor. E.g. Assessor > Verifier > Assessor (THIS ONE) > Verifier > Verified",
    'Verifier Assignment Date': "Date the chemical was first Assigned to a Verifier",
    'Last Verifier Date': "Most recent date the chemical was Assigned to a Verifier. E.g. Assessor > Verifier > Assessor > Verifier (THIS ONE) > Verified",
    'First Verified Date': "Date the chemical was first Verified. E.g. Verified (THIS ONE) > Back to Assessor > Verifier > Verified",
    'Last Verified Date': "Most recent date the chemical was Verified. E.g. Verified > Back to Assessor > Verifier > Verified (THIS ONE)"
}

for col_name, tip in hover_tips.items():
    if col_name in df.columns:
        cell = gspread.utils.rowcol_to_a1(1, df.columns.get_loc(col_name) + 1)
        worksheet.update_note(cell, tip)