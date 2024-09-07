import numpy as np
import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/05_gathered_data_merge_input.xlsx')
output_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/05_gathered_data_merge_output.xlsx')

df_output = pd.read_excel(input_file)

final_columns = {
    'final_id': ['pharos_id', 'cf_id'],
    'final_additional_casrns': [None, 'cf_additional_casrns'],
    'final_ec_number': [None, 'cf_ec_number'],
    'final_inci': [None, 'cf_inci'],
    'final_name': ['pharos_name', 'cf_name'],
    'final_status': ['pharos', 'cf_status'],
    'final_scil': ['pharos_scil_status', 'cf_scil_status'],
    'final_tco': ['pharos_tco_status', 'cf_tco_status'],
    'final_source': ['pharos', 'cf']
}

for final_col, [true_val, false_val] in final_columns.items():
    if true_val is None or true_val in ['pharos', 'cf']:
        true_condition = true_val
    else:
        true_condition = df_output[true_val]

    if false_val is None or false_val in ['pharos', 'cf']:
        false_condition = false_val
    else:
        false_condition = df_output[false_val]

    df_output[final_col] = np.where(df_output['query_required'], true_condition, false_condition)

df_output['final_hazard_band'] = np.where(
    (df_output['CF Data'] == 'no_cf_data'),
    df_output['pharos_hazard_band_score'],
    np.where(df_output['cf_list_based_hazard_score'].notna(), df_output['cf_list_based_hazard_score'], df_output['cf_manual_hazard_band_score'])
)

df_output['final_rollup_score'] = np.where(
    df_output['cf_list_based_c2c_score'].notna(),
    df_output['cf_list_based_c2c_score'],
    df_output['cf_manual_rollup_score']
)

no_data_condition = (df_output['Pharos Data'] == 'no_pharos_data') & (df_output['CF Data'] == 'no_cf_data')
df_output.loc[no_data_condition, 'final_status'] = ''
df_output.loc[no_data_condition, 'final_source'] = ''

columns_to_remove = [col for col in df_output.columns if col.startswith('cf_') or col.startswith('pharos_')] + ['Pharos Data', 'query_required', 'CF Data', 'Valid Corrected']
df_output.drop(columns=columns_to_remove, inplace=True)

df_output.to_excel(output_file, index=False)
