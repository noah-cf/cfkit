import pandas as pd
import utilities.connections as connections
import sqlalchemy
from tqdm import tqdm
import os
import json

conn = connections.get_db_engine('prod')

script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/02_fetch_additional_casrns_input.xlsx')
output_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/02_fetch_additional_casrns_output.xlsx')

sql_file_path = os.path.join(os.path.dirname(__file__), '../../../assets/ingredient_intelligence_report/02_fetch_additional_casrns.sql')
with open(sql_file_path, 'r') as file:
    query = file.read()

def fetch_all_profiles():
    result = pd.read_sql(query, conn)
    return result

def select_relevant_profile(cas, all_profiles):
    profiles = all_profiles[
        all_profiles['additional_casrn'].apply(lambda x: json.loads(x).get('additional_casrn')) == cas]

    if not profiles.empty:
        not_screening_only = profiles[profiles['status'] != 'screening_only']
        if not not_screening_only.empty:
            return not_screening_only.iloc[0]['cas_rn']

        with_list_score = profiles.dropna(subset=['list_based_hazard_score'])
        if not with_list_score.empty:
            return with_list_score.iloc[0]['cas_rn']

        with_manual_score = profiles[profiles['manual_hazard_band_score'] != '?']
        if not with_manual_score.empty:
            return with_manual_score.iloc[0]['cas_rn']

    return None

def process_cas_numbers(input_file, output_file):
    df = pd.read_excel(input_file)
    
    df['replaced?'] = False
    df['original_Final CAS'] = df['Final CAS']

    all_profiles = fetch_all_profiles()

    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing CAS numbers"):
        new_cas = select_relevant_profile(row['Final CAS'], all_profiles)
        if new_cas:
            df.at[index, 'Final CAS'] = new_cas
            df.at[index, 'replaced?'] = True

    cols = (
        ['replaced?', 'original_Final CAS', 'Final CAS'] +
        [col for col in df.columns if col not in ['replaced?', 'original_Final CAS', 'Final CAS']]
    )
    df = df[cols]

    df.to_excel(output_file, index=False)

process_cas_numbers(input_file, output_file)

