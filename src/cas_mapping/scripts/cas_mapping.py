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

def find_top3_matches(value, col):
    def calculate_distance(x):
        x_str = str(x) if x is not None else ""
        value_str = str(value) if value is not None else ""
        return levenshtein_distance(value_str, x_str)
    distances = col.apply(calculate_distance)
    sorted_indexes = distances.argsort()[:3]
    return distances[sorted_indexes], sorted_indexes

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
    
    output_data = {}
    sheet_names = ["first_distance_matches", "second_distance_matches", "third_distance_matches"]
    for sheet_name in sheet_names:
        output_data[sheet_name] = df_inci.copy()

    for idx, row in tqdm(df_inci.iterrows(), total=len(df_inci), desc="Processing ingredients"):
        ingredient = row['ingredients_lower']
        
        sources = [
            ("profiles_name", profiles_db["name_lower"], profiles_db, "profiles", "name"),
            ("profiles_inci", profiles_db["inci_lower"], profiles_db, "profiles", "inci"),
            ("cas_mapping", cas_mapping["name_lower"], cas_mapping, "mapping", "name")
        ]
        
        for source_name, source_col, source_df, map_source, map_type in sources:
            distances, indexes = find_top3_matches(ingredient, source_col)
            
            for i, (distance, index) in enumerate(zip(distances, indexes)):
                sheet_name = sheet_names[i]
                matched_row = source_df.iloc[index]
                
                priority_score = 0
                if source_df is profiles_db:
                    if matched_row['status'] != 'screening_only':
                        priority_score += 10
                    if pd.notna(matched_row['cas_rn']):
                        priority_score += 5
                
                current_score = output_data[sheet_name].loc[idx, "distance_lev"] if "distance_lev" in output_data[sheet_name].columns else float('inf')
                current_priority = output_data[sheet_name].loc[idx, "priority_score"] if "priority_score" in output_data[sheet_name].columns else -1
                
                if pd.isna(current_score) or distance < current_score or (distance == current_score and priority_score > current_priority):
                    output_data[sheet_name].loc[idx, "distance_lev"] = distance
                    output_data[sheet_name].loc[idx, "priority_score"] = priority_score
                    output_data[sheet_name].loc[idx, 'map_source'] = map_source
                    output_data[sheet_name].loc[idx, 'map_type'] = map_type
                    output_data[sheet_name].loc[idx, 'matched_on'] = source_col.iloc[index]
                    
                    if source_df is profiles_db:
                        output_data[sheet_name].loc[idx, 'map_casrn'] = str(matched_row['cas_rn'])
                        output_data[sheet_name].loc[idx, 'map_name'] = str(matched_row['name'])
                        output_data[sheet_name].loc[idx, 'map_inci'] = str(matched_row['inci'])
                        output_data[sheet_name].loc[idx, 'map_status'] = str(matched_row['status'])
                        output_data[sheet_name].loc[idx, 'map_hazard'] = str(matched_row['manual_hazard_band_score'])
                        output_data[sheet_name].loc[idx, 'map_c2c'] = str(matched_row['manual_rollup_score'])
                        output_data[sheet_name].loc[idx, 'map_scil'] = str(matched_row['scil_status'])
                        output_data[sheet_name].loc[idx, 'map_tco'] = str(matched_row['tco_status'])
                        output_data[sheet_name].loc[idx, 'map_additional_casrn'] = ','.join(matched_row['additional_casrns_list'])
                        output_data[sheet_name].loc[idx, 'map_list_hazard'] = str(matched_row['list_based_hazard_score'])
                        output_data[sheet_name].loc[idx, 'map_list_c2c'] = str(matched_row['list_based_c2c_score'])
                    else:
                        output_data[sheet_name].loc[idx, 'map_casrn'] = str(matched_row['casrn'])
                        output_data[sheet_name].loc[idx, 'map_name'] = str(matched_row['name'])
                        output_data[sheet_name].loc[idx, 'map_type'] = str(matched_row['type'])

    return output_data

def main():
    current_date = datetime.now().strftime('%Y_%m_%d')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, '../../../data/cas_mapping/DataPull_CF-Cleaned_10-10.xlsx')
    output_directory = os.path.join(script_dir, f'../../../data/cas_mapping/{current_date}_output_10-10')
    
    os.makedirs(output_directory, exist_ok=True)
    shutil.copy(input_file, os.path.join(output_directory, f'{current_date}_input_10-10.xlsx'))
    
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
    
    output_data = process_data(df_exploded, ingredients_column, products_column)
    
    for sheet_name, df in output_data.items():
        output_file = os.path.join(output_directory, f'{current_date}_{sheet_name}.xlsx')
        df.to_excel(output_file, index=False)
    
    print(f"Processing complete. Check the output files in {output_directory}")

if __name__ == "__main__":
    main()