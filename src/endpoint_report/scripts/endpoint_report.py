import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from utilities import connections

def save_df_to_csv(df, output_directory, base_filename):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"{base_filename}_{timestamp}.csv"
    filepath = os.path.join(output_directory, filename)
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

def fetch_latest_versions_verified():
    with open(os.path.join(os.path.dirname(__file__), '../../../assets/endpoint_report/fetch_latest_verions_verified.sql'), 'r') as file:
        query = file.read()
    df = pd.read_sql(query, connections.get_db_engine('prod'))
    return df

def parse_json_in_column(df, column_name='document'):
    try:
        if isinstance(df[column_name].iloc[0], str):
            df[column_name] = df[column_name].apply(json.loads)
    except KeyError:
        print(f"Column '{column_name}' not found in DataFrame.")
    except Exception as e:
        print(f"Error parsing JSON in column '{column_name}': {e}")
    return df

def extract_ratings_data(df, document_column='document'):
    df = parse_json_in_column(df, document_column)
    ghs_data, mw_data = [], []

    for _, row in df.iterrows():
        document = row[document_column]
        profile_id = row['profile_id']
        cas_rn = row['cas_rn']
        inserted_at = row['inserted_at']
        for key, value in document.items():
            if key.startswith("dedupe_") and isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if 'ghs' in sub_key or 'mw' in sub_key:
                        entry = {
                            'profile_id': profile_id,
                            'cas_rn': cas_rn,
                            'inserted_at': inserted_at,
                            'dedupe_key': key,
                            'rating_key': sub_key,
                            'sub_values': sub_value
                        }
                        if 'ghs' in sub_key:
                            ghs_data.append(entry)
                        elif 'mw' in sub_key:
                            mw_data.append(entry)

    return pd.DataFrame(ghs_data), pd.DataFrame(mw_data)

def process_ghs_ratings(ghs_df):
    ghs_columns = [
        'ghs_oral_carcinogenicity',
        'ghs_dermal_carcinogenicity',
        'ghs_inhalation_carcinogenicity',
        'ghs_mutagenicity',
        'ghs_oral_reproductive_toxicity',
        'ghs_dermal_reproductive_toxicity',
        'ghs_inhalation_reproductive_toxicity',
        'ghs_oral_developmental_toxicity',
        'ghs_dermal_developmental_toxicity',
        'ghs_inhalation_developmental_toxicity',
        'ghs_oral_toxicity',
        'ghs_dermal_toxicity',
        'ghs_inhalation_toxicity',
        'ghs_oral_exposure',
        'ghs_dermal_exposure',
        'ghs_inhalation_exposure',
        'ghs_oral_chronic',
        'ghs_dermal_chronic',
        'ghs_inhalation_chronic',
        'ghs_oral_neurotoxicity_single',
        'ghs_dermal_neurotoxicity_single',
        'ghs_inhalation_neurotoxicity_single',
        'ghs_oral_neurotoxicity_repeated',
        'ghs_dermal_neurotoxicity_repeated',
        'ghs_inhalation_neurotoxicity_repeated',
        'ghs_skin_sensitization',
        'ghs_respiratory_sensitization',
        'ghs_skin_respiratory_sensitization',
        'ghs_skin_irritation',
        'ghs_eye_irritation',
        'ghs_respiratory_irritation',
        'ghs_acute_aquatic',
        'ghs_chronic_aquatic',
        'ghs_ozone_layer',
        'ghs_explosives',
        'ghs_flammable_gases',
        'ghs_aerosols',
        'ghs_oxidizing_gases',
        'ghs_flammable_liquids',
        'ghs_flammable_solids',
        'ghs_self_reactive_substances',
        'ghs_pyrophoric_liquids',
        'ghs_pyrophoric_solids',
        'ghs_self_heating_substances',
        'ghs_oxidizing_liquids',
        'ghs_oxidizing_solids',
        'ghs_organic_peroxides',
        'ghs_corrosive_to_metals',
        'ghs_desensitized_explosives',
        'ghs_lactation_toxicity',
        'ghs_aspiration'
    ]
    
    organs = ['lung', 'liver', 'blood', 'kidney', 'other', 'none']
    organ_fields = ['rating', 'single_ghs', 'repeated_ghs', 'other_organ']
    
    organ_toxicity_columns = []
    for organ in organs:
        for field in organ_fields:
            column_name = f'ghs_organ_toxicity_{organ}_{field}'
            organ_toxicity_columns.append(column_name)
    
    ghs_aquatic_subvalues = [
        'algae_ghs',
        'other_ghs',
        'crustacea_ghs',
        'vertebrate_ghs'
    ]
    
    base_columns = ['profile_id', 'cas_rn', 'inserted_at']
    all_columns = base_columns + ghs_columns + organ_toxicity_columns + [f'acute_{col}' for col in ghs_aquatic_subvalues] + [f'chronic_{col}' for col in ghs_aquatic_subvalues]
    
    profile_ratings = {}
    
    for index, row in tqdm(ghs_df.iterrows(), total=ghs_df.shape[0], desc="Processing GHS Ratings"):
        profile_id = row['profile_id']
        if profile_id not in profile_ratings:
            profile_ratings[profile_id] = {col: None for col in all_columns}
            profile_ratings[profile_id].update({'profile_id': profile_id, 'cas_rn': row['cas_rn'], 'inserted_at': row['inserted_at']})
        
        rating_key = row['rating_key']
        sub_values = row['sub_values']
        
        if rating_key in ghs_columns and 'rating' in sub_values and isinstance(sub_values['rating'], dict):
            rating_value = sub_values['rating'].get('rating')
            profile_ratings[profile_id][rating_key] = rating_value
        
        if rating_key == 'ghs_organ_toxicity' and isinstance(sub_values, list):
            for organ_entry in sub_values:
                if isinstance(organ_entry, dict):
                    organ = organ_entry.get('organ')
                    
                    organ_key = 'none' if organ is None else organ.lower()
                    
                    if organ_key not in organs:
                        print(f"Warning: Unexpected organ '{organ}' found, skipping...")
                        continue
                    
                    rating_info = organ_entry.get('rating', {})
                    rating_value = rating_info.get('rating') if isinstance(rating_info, dict) else None
                    
                    profile_ratings[profile_id][f'ghs_organ_toxicity_{organ_key}_rating'] = rating_value
                    profile_ratings[profile_id][f'ghs_organ_toxicity_{organ_key}_single_ghs'] = organ_entry.get('single_ghs')
                    profile_ratings[profile_id][f'ghs_organ_toxicity_{organ_key}_repeated_ghs'] = organ_entry.get('repeated_ghs')
                    profile_ratings[profile_id][f'ghs_organ_toxicity_{organ_key}_other_organ'] = organ_entry.get('other_organ')
        
        if rating_key == 'ghs_acute_aquatic' and isinstance(sub_values, dict):
            for subvalue_key in ghs_aquatic_subvalues:
                profile_ratings[profile_id][f'acute_{subvalue_key}'] = sub_values.get(subvalue_key)
        if rating_key == 'ghs_chronic_aquatic' and isinstance(sub_values, dict):
            for subvalue_key in ghs_aquatic_subvalues:
                profile_ratings[profile_id][f'chronic_{subvalue_key}'] = sub_values.get(subvalue_key)
    
    processed_ghs_df = pd.DataFrame(list(profile_ratings.values()), columns=all_columns)
    return processed_ghs_df

def process_mw_ratings(mw_df):
    mw_columns = [
        'mw_oral_carcinogenicity',
        'mw_dermal_carcinogenicity',
        'mw_inhalation_carcinogenicity',
        'mw_mutagenicity',
        'mw_oral_reproductive_developmental_toxicity',
        'mw_dermal_reproductive_developmental_toxicity',
        'mw_inhalation_reproductive_developmental_toxicity',
        'mw_endocrine_disruption',
        'mw_oral_toxicity',
        'mw_dermal_toxicity',
        'mw_inhalation_toxicity',
        'mw_oral_neurotoxicity',
        'mw_dermal_neurotoxicity',
        'mw_inhalation_neurotoxicity',
        'mw_corrosion_irritation',
        'mw_sensitizing_effects',
        'mw_aquatic_fish',
        'mw_aquatic_fish_noec',
        'mw_aquatic_daphnia',
        'mw_aquatic_daphnia_noec',
        'mw_aquatic_algae',
        'mw_aquatic_algae_noec',
        'mw_terrestrial_toxicity',
        'mw_persistence_biodegradation',
        'mw_bioaccumulation',
        'mw_climatic_relevance',
        'mw_other_human',
        'mw_organohalogens',
        'mw_toxic_metals',
        'mw_other_environmental'
    ]
    mw_persistence_subvalues = [
        'no_data',
        'thalf_air',
        'thalf_soil',
        'air_dominant',
        'soil_dominant',
        'water_dominant',
        'air_distribution',
        'override_default',
        'thalf_air_method',
        'sediment_dominant',
        'soil_distribution',
        'thalf_fresh_water',
        'thalf_soil_method',
        'thalf_marine_water',
        'water_distribution',
        'thalf_fresh_sediment',
        'sediment_distribution',
        'thalf_marine_sediment',
        'thalf_fresh_water_method',
        'thalf_marine_water_method',
        'thalf_fresh_sediment_method',
        'thalf_marine_sediment_method',
        'ready_biodegradability_results',
        'inherent_biodegradation_results'
    ]
    mw_bioaccumulation_subvalues = [
        'bxf',
        'weight',
        'log_kow',
        'no_data',
        'bxf_type',
        'log_kow_type',
        'override_default'
    ]
    base_columns = ['profile_id', 'cas_rn', 'inserted_at']
    all_columns = base_columns + mw_columns + [f'persistence_{col}' for col in mw_persistence_subvalues] + [f'bioaccumulation_{col}' for col in mw_bioaccumulation_subvalues]
    profile_ratings = {}
    for _, row in tqdm(mw_df.iterrows(), total=mw_df.shape[0], desc="Processing MW Ratings"):
        profile_id = row['profile_id']
        if profile_id not in profile_ratings:
            profile_ratings[profile_id] = {col: None for col in all_columns}
            profile_ratings[profile_id].update({'profile_id': profile_id, 'cas_rn': row['cas_rn'], 'inserted_at': row['inserted_at']})
        rating_key = row['rating_key']
        sub_values = row['sub_values']
        if rating_key in mw_columns and isinstance(sub_values, dict) and 'rating' in sub_values and isinstance(sub_values['rating'], dict) and sub_values['rating'].get('system') == 'mw':
            rating_value = sub_values['rating']['rating']
            profile_ratings[profile_id][rating_key] = rating_value
        if rating_key == 'mw_persistence_biodegradation' and isinstance(sub_values, dict):
            for subvalue_key in mw_persistence_subvalues:
                profile_ratings[profile_id][f'persistence_{subvalue_key}'] = sub_values.get(subvalue_key)
        if rating_key == 'mw_bioaccumulation' and isinstance(sub_values, dict):
            for subvalue_key in mw_bioaccumulation_subvalues:
                profile_ratings[profile_id][f'bioaccumulation_{subvalue_key}'] = sub_values.get(subvalue_key)
        if rating_key == 'mw_aquatic_fish' and 'noec' in sub_values:
            profile_ratings[profile_id]['mw_aquatic_fish_noec'] = sub_values['noec']
        if rating_key == 'mw_aquatic_algae' and 'noec' in sub_values:
            profile_ratings[profile_id]['mw_aquatic_algae_noec'] = sub_values['noec']
        if rating_key == 'mw_aquatic_daphnia' and 'noec' in sub_values:
            profile_ratings[profile_id]['mw_aquatic_daphnia_noec'] = sub_values['noec']

    processed_mw_df = pd.DataFrame(list(profile_ratings.values()), columns=all_columns)
    return processed_mw_df

if __name__ == "__main__":
    base_path = os.path.join(os.path.dirname(__file__), '../../../data/endpoint_reports')
    current_date_str = datetime.now().strftime('%Y-%m-%d')
    output_directory = os.path.join(base_path, current_date_str)
    os.makedirs(output_directory, exist_ok=True)

    latest_versions_dir = os.path.join(output_directory, 'latest_versions_verified')
    endpoints_dir = os.path.join(output_directory, 'endpoints')
    endpoint_ratings_dir = os.path.join(output_directory, 'endpoint_ratings')
    os.makedirs(latest_versions_dir, exist_ok=True)
    os.makedirs(endpoints_dir, exist_ok=True)
    os.makedirs(endpoint_ratings_dir, exist_ok=True)

    df = fetch_latest_versions_verified()
    if not df.empty:
        print("Saving latest versions verified...")
        save_df_to_csv(df, latest_versions_dir, 'latest_versions_verified')
        
        print("Extracting GHS and MW data from documents...")
        ghs_data, mw_data = extract_ratings_data(df)
        
        print("Saving GHS and MW endpoints...")
        save_df_to_csv(ghs_data, endpoints_dir, 'ghs_endpoints')
        save_df_to_csv(mw_data, endpoints_dir, 'mw_endpoints')
        
        print("Processing GHS ratings...")
        processed_ghs_data = process_ghs_ratings(ghs_data)
        print("Saving GHS ratings...")
        save_df_to_csv(processed_ghs_data, endpoint_ratings_dir, 'ghs_ratings')
        
        print("Processing MW ratings...")
        processed_mw_data = process_mw_ratings(mw_data)
        print("Saving MW ratings...")
        save_df_to_csv(processed_mw_data, endpoint_ratings_dir, 'mw_ratings')
    else:
        print("No data fetched, nothing to process.")