import pandas as pd
from Levenshtein import distance as levenshtein_distance
from tqdm import tqdm
import os
import shutil
import re
from datetime import datetime
from openpyxl import load_workbook

current_date = datetime.now().strftime('%Y_%m_%d')

input_file_path = os.path.join(os.path.dirname(__file__), '../../../data/cas_mapping/input_9.xlsx')
output_directory = os.path.join(os.path.dirname(__file__), f'../../../data/cas_mapping/{current_date}_output_9')
pharos_profiles_path = os.path.join(os.path.dirname(__file__), '../../../assets/cas_mapping/pharos_profiles.csv')
cas_mapping_path = os.path.join(os.path.dirname(__file__), '../../../assets/cas_mapping/cas_maps.xlsx')

os.makedirs(output_directory, exist_ok=True)
shutil.copy(input_file_path, os.path.join(output_directory, f'{current_date}_input.xlsx'))

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
        "may vary in color and consistency", "usda", "love & pride.", "love & pride", "()"
    ]
    for phrase in phrases_to_remove:
        ingredients = ingredients.replace(phrase, '')
    ingredients = ingredients.lower()
    ingredients = re.sub(r'[^a-zA-Z0-9\s(),-]', '', ingredients)
    ingredients = ingredients.strip()
    return ingredients

def find_top3_matches(value, col):
    def calculate_distance(x):
        x_str = str(x) if x is not None else ""
        value_str = str(value) if value is not None else ""
        return levenshtein_distance(value_str, x_str)
    distances = col.apply(calculate_distance)
    sorted_indexes = distances.argsort()[:3]
    return distances[sorted_indexes], sorted_indexes

def process_all():
    print("Reading mapping data...")
    profiles_pharos = pd.read_csv(pharos_profiles_path, dtype=str)
    cas_mapping = pd.read_excel(cas_mapping_path, sheet_name="mappings", engine='openpyxl', dtype=str)
    profiles_pharos["name_lower"] = profiles_pharos["name"].str.lower()
    profiles_pharos["inci_lower"] = profiles_pharos["inci"].str.lower()
    cas_mapping["name_lower"] = cas_mapping["name"].str.lower()

    file_extension = input_file_path.split('.')[-1]
    output_file = os.path.join(output_directory, f'{current_date}_final_output.{file_extension}')
    chunksize = 2500

    first_chunk = (pd.read_excel(input_file_path, nrows=5)
                   if file_extension == 'xlsx'
                   else pd.read_csv(input_file_path, nrows=5))
    ingredients_column = get_column_name(first_chunk, "Ingredients Column", "Enter the column name containing ingredients:")
    products_column = get_column_name(first_chunk, "Products Column", "Enter the column name containing product names:")

    mode = 'w'
    if file_extension == 'csv':
        reader = pd.read_csv(input_file_path, chunksize=chunksize)
    else:
        df = pd.read_excel(input_file_path)
        reader = (df.iloc[i:i+chunksize] for i in range(0, len(df), chunksize))

    for chunk in tqdm(reader, desc="Processing chunks"):
        chunk = chunk.copy()

        chunk['full_ingredients_list'] = chunk[ingredients_column]
        chunk['cleaned_ingredients'] = chunk[ingredients_column].apply(clean_ingredients)
        split_delimiters = r',\s+'
        chunk['cleaned_ingredients'] = chunk['cleaned_ingredients'].str.split(split_delimiters)
        chunk_exploded = chunk.explode('cleaned_ingredients')
        chunk_exploded['product_index'] = chunk_exploded.groupby(products_column).ngroup() + 1
        chunk_exploded['full_ingredients_list'] = chunk_exploded.groupby(products_column)['full_ingredients_list'].transform('first')
        chunk_exploded.loc[chunk_exploded.duplicated(products_column), 'full_ingredients_list'] = ''
        chunk_exploded['ingredient_order'] = chunk_exploded.groupby(products_column).cumcount() + 1
        chunk_exploded = chunk_exploded.reset_index(drop=True)
        chunk_exploded['ingredient_index'] = chunk_exploded.index + 1

        chunk_exploded['ingredient_lower'] = chunk_exploded['cleaned_ingredients'].str.lower()

        for idx, row in tqdm(chunk_exploded.iterrows(), total=len(chunk_exploded), desc="Matching CAS numbers", leave=False):
            ingredient = row["ingredient_lower"]
            dists, idxs = find_top3_matches(ingredient, profiles_pharos["name_lower"])
            best_distance = dists.iloc[0] if not dists.empty else None
            chunk_exploded.at[idx, "distance_lev"] = best_distance
            if len(idxs) > 0:
                best_match = profiles_pharos.iloc[idxs[0]]
                chunk_exploded.at[idx, "map_casrn"] = best_match["casrn"]
                chunk_exploded.at[idx, "map_source"] = "pharos"
                chunk_exploded.at[idx, "map_type"] = "pharos"
            else:
                chunk_exploded.at[idx, "map_casrn"] = ""
                chunk_exploded.at[idx, "map_source"] = "none"
                chunk_exploded.at[idx, "map_type"] = ""

        if output_file.endswith('.xlsx'):
            if mode == 'w':
                with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
                    chunk_exploded.to_excel(writer, sheet_name='Final Data', index=False)
            else:
                with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                    startrow = writer.sheets['Final Data'].max_row if 'Final Data' in writer.sheets else 0
                    chunk_exploded.to_excel(writer, sheet_name='Final Data', index=False,
                                            header=False, startrow=startrow)
        else:
            chunk_exploded.to_csv(output_file, index=False, mode=mode, header=(mode=='w'))
        mode = 'a'
    
    print(f"Processing complete. Output saved at {output_file}")

if __name__ == "__main__":
    process_all()
