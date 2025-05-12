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

def get_original_profile_data(cas):
    """Fetch profile data for the original CAS using a direct query"""
    query = f"""
    SELECT 
        cas_rn, 
        name,
        inci,
        status, 
        list_based_hazard_score, 
        manual_hazard_band_score,
        manual_rollup_score,
        list_based_c2c_score
    FROM profiles
    WHERE cas_rn = %s
    LIMIT 1
    """
    
    result = pd.read_sql(query, conn, params=(cas,))
    if not result.empty:
        return result.iloc[0]
    return None

def display_profile_info(profile_data, original_cas=None):
    """Display profile information in a formatted way"""
    print("\n" + "-" * 50)
    if original_cas:
        print(f"Original CAS: {original_cas}")
    print(f"Profile CAS: {profile_data.get('cas_rn', 'N/A')}")
    print(f"Name: {profile_data.get('name', 'N/A')}")
    print(f"INCI: {profile_data.get('inci', 'N/A')}")
    print(f"Status: {profile_data.get('status', 'N/A')}")
    print(f"List-based Hazard: {profile_data.get('list_based_hazard_score', 'N/A')}")
    print(f"Manual Hazard: {profile_data.get('manual_hazard_band_score', 'N/A')}")
    print(f"List-based C2C: {profile_data.get('list_based_c2c_score', 'N/A')}")
    print(f"Manual C2C: {profile_data.get('manual_rollup_score', 'N/A')}")
    
    status = profile_data.get('status')
    if status == 'verified':
        print("*** FULL CHA (Verified) ***")
    elif status and status != 'screening_only':
        print("*** CHA IN PROGRESS ***")
    print("-" * 50)

def select_relevant_profile(cas, all_profiles):
    """Return both the selected CAS and the full profile data"""
    profiles = all_profiles[
        all_profiles['additional_casrn'].apply(lambda x: json.loads(x).get('additional_casrn')) == cas]

    if not profiles.empty:
        not_screening_only = profiles[profiles['status'] != 'screening_only']
        if not not_screening_only.empty:
            selected = not_screening_only.iloc[0]
            return selected['cas_rn'], selected
            
        with_list_score = profiles.dropna(subset=['list_based_hazard_score'])
        if not with_list_score.empty:
            selected = with_list_score.iloc[0]
            return selected['cas_rn'], selected
            
        with_manual_score = profiles[profiles['manual_hazard_band_score'] != '?']
        if not with_manual_score.empty:
            selected = with_manual_score.iloc[0]
            return selected['cas_rn'], selected
            
        selected = profiles.iloc[0]
        return selected['cas_rn'], selected

    return None, None

def process_cas_numbers(input_file, output_file):
    df = pd.read_excel(input_file)
    
    df['replaced?'] = False
    df['original_Final CAS'] = df['Final CAS']
    df['suggested_Final CAS'] = None

    all_profiles = fetch_all_profiles()

    user_choice = input("Do you want to replace Final CAS numbers with alternative CAS numbers? (1: No, 2: Yes, 3: Interactive): ").strip()
    
    if user_choice == "ABORT PROCESS":
        print("\nProcess aborted by user. Saving current state...")
        df.to_excel(output_file, index=False)
        print(f"Output saved to: {output_file}")
        return
    
    if user_choice == '1':
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing CAS numbers"):
            new_cas, _ = select_relevant_profile(row['Final CAS'], all_profiles)
            
            if new_cas:
                df.at[index, 'suggested_Final CAS'] = new_cas
    
    elif user_choice == '2':
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing CAS numbers"):
            new_cas, _ = select_relevant_profile(row['Final CAS'], all_profiles)
            
            if new_cas:
                df.at[index, 'suggested_Final CAS'] = new_cas
                df.at[index, 'Final CAS'] = new_cas
                df.at[index, 'replaced?'] = True
    
    elif user_choice == '3':
        df_no_replace = df.copy()
        df_replace = df.copy()
        profile_data_map = {}
        
        for index, row in tqdm(df_no_replace.iterrows(), total=df_no_replace.shape[0], desc="Processing CAS numbers (no replace)"):
            new_cas, profile_data = select_relevant_profile(row['Final CAS'], all_profiles)
            
            if new_cas:
                df_no_replace.at[index, 'suggested_Final CAS'] = new_cas
                profile_data_map[index] = profile_data
        
        for index, row in tqdm(df_replace.iterrows(), total=df_replace.shape[0], desc="Processing CAS numbers (replace)"):
            new_cas, _ = select_relevant_profile(row['Final CAS'], all_profiles)
            
            if new_cas:
                df_replace.at[index, 'suggested_Final CAS'] = new_cas
                df_replace.at[index, 'Final CAS'] = new_cas
                df_replace.at[index, 'replaced?'] = True
        
        df_no_replace_filtered = df_no_replace[df_no_replace['suggested_Final CAS'].notna()]
        df_replace_filtered = df_replace[df_replace['replaced?'] == True]
        
        print(f"\nFound {len(df_no_replace_filtered)} items with replacement options.")
        
        df_final = df.copy()
        
        for idx, (index, row_no_replace) in enumerate(df_no_replace_filtered.iterrows()):
            row_replace = df_replace_filtered.loc[index]
            
            print(f"\n{'='*80}")
            print(f"Item {idx+1} of {len(df_no_replace_filtered)}")
            
            if 'ingr_name' in row_no_replace:
                print(f"Ingredient name (from input): {row_no_replace['ingr_name']}")
            
            original_cas = row_no_replace['original_Final CAS']
            original_profile_data = get_original_profile_data(original_cas)
            
            print("\nCURRENT PROFILE:")
            if original_profile_data is not None:
                display_profile_info(original_profile_data.to_dict())
            else:
                print(f"CAS: {original_cas} (No ChemFORWARD profile)")
            
            print("\nSUGGESTED REPLACEMENT:")
            replacement_profile_data = profile_data_map.get(index)
            if replacement_profile_data is not None:
                display_profile_info(replacement_profile_data.to_dict(), original_cas=original_cas)
            
            print("\n" + "="*80)
            choice = input("Replace this CAS? (y/n): ").strip()
            
            if choice == "ABORT PROCESS":
                print("\nProcess aborted by user. Saving current state...")
                df = df_final
                cols = (
                    ['replaced?', 'original_Final CAS', 'suggested_Final CAS', 'Final CAS'] +
                    [col for col in df.columns if col not in ['replaced?', 'original_Final CAS', 'suggested_Final CAS', 'Final CAS']]
                )
                df = df[cols]
                df.to_excel(output_file, index=False)
                print(f"Output saved to: {output_file}")
                replaced_count = df['replaced?'].sum()
                print(f"\nSummary: Replaced {replaced_count} items before aborting.")
                return
            
            if choice.lower() == 'y':
                df_final.at[index, 'Final CAS'] = row_replace['Final CAS']
                df_final.at[index, 'replaced?'] = True
                df_final.at[index, 'suggested_Final CAS'] = row_replace['suggested_Final CAS']
            else:
                df_final.at[index, 'suggested_Final CAS'] = row_no_replace['suggested_Final CAS']
        
        df = df_final
    
    else:
        print("Invalid choice. Using option 1 (no replacement).")
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing CAS numbers"):
            new_cas, _ = select_relevant_profile(row['Final CAS'], all_profiles)
            
            if new_cas:
                df.at[index, 'suggested_Final CAS'] = new_cas
    
    cols = (
        ['replaced?', 'original_Final CAS', 'suggested_Final CAS', 'Final CAS'] +
        [col for col in df.columns if col not in ['replaced?', 'original_Final CAS', 'suggested_Final CAS', 'Final CAS']]
    )
    df = df[cols]

    df.to_excel(output_file, index=False)
    
    replaced_count = df['replaced?'].sum()
    total_count = len(df)
    print(f"\nSummary: Replaced {replaced_count} out of {total_count} CAS numbers.")

process_cas_numbers(input_file, output_file)