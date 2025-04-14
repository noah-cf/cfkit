import pandas as pd
from Levenshtein import distance as levenshtein_distance
from tqdm import tqdm
import os
import shutil
import re
import json
from datetime import datetime
from utilities import connections

def get_column_name(df, title, message):
    print(f"{title}: {message}")
    column_name = input("Enter the column name: ")
    if column_name and column_name in df.columns:
        return column_name
    else:
        print(f"Error: Column '{column_name}' not found or no column name provided. Exiting.")
        exit()

def clean_ingredients(ingredients):
    ingredients = str(ingredients)
    ingredients = ingredients.encode("utf-8", "ignore").decode("utf-8", "ignore")
    ingredients = ingredients.replace('*', '').replace(':', '').replace('±', '')
    phrases_to_remove = [
        "peut contenir", "fair trade ingredient", "parfum/fragrance",
        "(parfum)fragrance", "fragrance (parfum)", "(parfum) fragrance",
        "certified organic ingredient", "certified organic ingredients",
        "certified organicingredients", "natural ingredients",
        "may vary in color and consistency", "usda", "love & pride.", 
        "love & pride", "()"
    ]
    for phrase in phrases_to_remove:
        ingredients = ingredients.replace(phrase, '')
    ingredients = ingredients.lower()
    ingredients = re.sub(r'[^a-zA-Z0-9\s(),-]', '', ingredients)
    ingredients = ingredients.strip()
    return ingredients

def extract_additional_casrns(json_str):
    try:
        if isinstance(json_str, bytes):
            json_str = json_str.decode()
        if not json_str or json_str == '[]' or json_str == 'null':
            return []
        data = json.loads(json_str)
        return [item['additional_casrn'] for item in data if 'additional_casrn' in item]
    except:
        return []

def find_best_match(value, profiles_db, cas_mapping):
    """
    Find the best match for an ingredient across all sources.
    Returns a dictionary with the match information.
    """
    value_str = str(value) if value is not None else ""
    best_match = {
        'distance_lev': float('inf'),
        'priority_score': -1,
        'map_source': None,
        'map_type': None,
        'matched_on': None,
        'map_casrn': None,
        'map_name': None,
        'map_inci': None,
        'map_status': None,
        'map_hazard': None,
        'map_c2c': None,
        'map_scil': None,
        'map_tco': None,
        'map_additional_casrn': None,
        'map_list_hazard': None,
        'map_list_c2c': None
    }
    
    sources = [
        ("profiles_name", profiles_db["name_lower"], profiles_db, "profiles", "name"),
        ("profiles_inci", profiles_db["inci_lower"], profiles_db, "profiles", "inci"),
        ("cas_mapping", cas_mapping["name_lower"], cas_mapping, "mapping", "name")
    ]
    
    for source_name, source_col, source_df, map_source, map_type in sources:
        distances = source_col.apply(lambda x: levenshtein_distance(value_str, str(x) if x is not None else ""))
        min_index = distances.argmin()
        min_distance = distances[min_index]
        
        priority_score = 0
        if source_df is profiles_db:
            matched_row = source_df.iloc[min_index]
            if matched_row['status'] != 'screening_only':
                priority_score += 10
            if pd.notna(matched_row['cas_rn']):
                priority_score += 5
        
        if (min_distance < best_match['distance_lev'] or 
            (min_distance == best_match['distance_lev'] and priority_score > best_match['priority_score'])):
            
            best_match['distance_lev'] = min_distance
            best_match['priority_score'] = priority_score
            best_match['map_source'] = map_source
            best_match['map_type'] = map_type
            best_match['matched_on'] = source_col.iloc[min_index]
            
            matched_row = source_df.iloc[min_index]
            if source_df is profiles_db:
                best_match['map_casrn'] = str(matched_row['cas_rn'])
                best_match['map_name'] = str(matched_row['name'])
                best_match['map_inci'] = str(matched_row['inci'])
                best_match['map_status'] = str(matched_row['status'])
                best_match['map_hazard'] = str(matched_row['manual_hazard_band_score'])
                best_match['map_c2c'] = str(matched_row['manual_rollup_score'])
                best_match['map_scil'] = str(matched_row['scil_status'])
                best_match['map_tco'] = str(matched_row['tco_status'])
                best_match['map_additional_casrn'] = ','.join(matched_row['additional_casrns_list'])
                best_match['map_list_hazard'] = str(matched_row['list_based_hazard_score'])
                best_match['map_list_c2c'] = str(matched_row['list_based_c2c_score'])
            else:
                best_match['map_casrn'] = str(matched_row['casrn'])
                best_match['map_name'] = str(matched_row['name'])
                best_match['map_type'] = str(matched_row['type'])
                best_match['map_inci'] = None
                best_match['map_status'] = None
                best_match['map_hazard'] = None
                best_match['map_c2c'] = None
                best_match['map_scil'] = None
                best_match['map_tco'] = None
                best_match['map_additional_casrn'] = None
                best_match['map_list_hazard'] = None
                best_match['map_list_c2c'] = None
            
            if min_distance == 0:
                break
    
    return best_match

def process_data(df_inci, ingredients_column, products_column):
    query = """
    SELECT 
        id,
        name,
        cas_rn,
        inci,
        status,
        manual_rollup_score,
        manual_hazard_band_score,
        scil_status,
        tco_status,
        additional_casrns,
        list_based_c2c_score,
        list_based_hazard_score
    FROM profiles
    """
    
    engine = connections.get_db_engine('prod')
    profiles_db = pd.read_sql(query, engine)
    
    cas_mapping_path = os.path.join(os.path.dirname(__file__), '../../../assets/cas_mapping/cas_maps.xlsx')
    cas_mapping = pd.read_excel(cas_mapping_path, sheet_name="mappings", engine='openpyxl', dtype=str)
    
    profiles_db['additional_casrns_list'] = profiles_db['additional_casrns'].apply(extract_additional_casrns)
    
    profiles_db['name_lower'] = profiles_db['name'].str.lower()
    profiles_db['inci_lower'] = profiles_db['inci'].str.lower()
    cas_mapping['name_lower'] = cas_mapping['name'].str.lower()
    
    ingredient_match_cache = {}
    
    output_data = df_inci.copy()
    
    unique_ingredients = df_inci['ingredients_lower'].unique()
    
    for ingredient in tqdm(unique_ingredients, desc="Processing unique ingredients"):
        if pd.isna(ingredient) or ingredient == '':
            continue
            
        best_match = find_best_match(ingredient, profiles_db, cas_mapping)
        
        ingredient_match_cache[ingredient] = best_match
    
    for idx, row in tqdm(df_inci.iterrows(), total=len(df_inci), desc="Applying matches to all ingredients"):
        ingredient = row['ingredients_lower']
        
        if pd.isna(ingredient) or ingredient == '' or ingredient not in ingredient_match_cache:
            continue
            
        match = ingredient_match_cache[ingredient]
        
        for key, value in match.items():
            output_data.loc[idx, key] = value
    
    return output_data

def main():
    current_date = datetime.now().strftime('%Y_%m_%d')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, '../../../data/cas_mapping/input.xlsx')
    output_directory = os.path.join(script_dir, f'../../../data/cas_mapping/{current_date}_output')
    
    os.makedirs(output_directory, exist_ok=True)
    shutil.copy(input_file, os.path.join(output_directory, f'{current_date}_input.xlsx'))
    
    if input_file.endswith('.xlsx'):
        df = pd.read_excel(input_file)
    elif input_file.endswith('.csv'):
        df = pd.read_csv(input_file)
    else:
        raise ValueError("Invalid file format. Please provide an Excel (.xlsx) or CSV (.csv) file.")
    
    ingredients_column = get_column_name(df, "Ingredients Column", "Enter the column name containing ingredients:")
    products_column = get_column_name(df, "Products Column", "Enter the column name containing product names:")
    
    df['full_ingredients_list'] = df[ingredients_column]
    df['ingredients_lower'] = df[ingredients_column].apply(clean_ingredients)
    df['ingredients_lower'] = df['ingredients_lower'].str.split(r',\s+')
    
    df_exploded = df.explode('ingredients_lower')
    df_exploded['product_index'] = df_exploded.groupby(products_column).ngroup() + 1
    df_exploded['ingredient_order'] = df_exploded.groupby(products_column).cumcount() + 1
    df_exploded = df_exploded.reset_index(drop=True)
    df_exploded['ingredient_index'] = df_exploded.index + 1
    
    output_df = process_data(df_exploded, ingredients_column, products_column)
    
    output_file = os.path.join(output_directory, f'{current_date}_best_matches.xlsx')
    output_df.to_excel(output_file, index=False)
    
    print(f"Processing complete. Check the output file at {output_file}")

if __name__ == "__main__":
    main()