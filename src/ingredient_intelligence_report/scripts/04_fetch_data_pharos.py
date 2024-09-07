import numpy as np
import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/04_fetch_data_pharos_input.xlsx')
pharos_file = os.path.join(script_dir, '../../../assets/pharos.xlsx')
output_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/04_fetch_data_pharos_output.xlsx')
cas_column = 'Final CAS'

df_input = pd.read_excel(input_file)
df_pharos = pd.read_excel(pharos_file)

df_input['query_required'] = (df_input['CF Data'] == 'no_cf_data') & (
    ~df_input[cas_column].isin(['invalid_cas', 'no_cas']))

df_merged = pd.merge(df_input, df_pharos, how='left', left_on=cas_column, right_on='casrn')

df_merged["Pharos Data"] = np.where(df_merged['query_required'] & df_merged['casrn'].notna(), "pharos_data",
                                    "no_pharos_data")

pharos_columns = df_pharos.columns.tolist()
df_merged.rename(columns={col: 'pharos_' + col for col in pharos_columns}, inplace=True)

df_merged.to_excel(output_file, index=False)
