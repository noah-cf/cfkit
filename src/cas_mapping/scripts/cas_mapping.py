import pandas as pd
from Levenshtein import distance as levenshtein_distance
from tqdm import tqdm
import os
import shutil
import re
from datetime import datetime

current_date = datetime.now().strftime('%Y_%m_%d')

input_file_path = os.path.join(os.path.dirname(__file__), '../../../data/cas_mapping/input.xlsx')
output_directory = os.path.join(os.path.dirname(__file__), f'../../../data/cas_mapping/{current_date}_output')
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
        "peut contenir",
        "fair trade ingredient",
        "parfum/fragrance",
        "(parfum)fragrance",
        "fragrance (parfum)",
        "(parfum) fragrance",
        "certified organic ingredient",
        "certified organic ingredients",
        "certified organicingredients",
        "natural ingredients",
        "may vary in color and consistency",
        "usda",
        "love & pride.",
        "love & pride",
        "()"
    ]
    for phrase in phrases_to_remove:
        ingredients = ingredients.replace(phrase, '')

    ingredients = ingredients.lower()
    ingredients = re.sub(r'[^a-zA-Z0-9\s(),-]', '', ingredients)
    ingredients = ingredients.strip()

    return ingredients

def process_inci():
    if not os.path.exists(input_file_path):
        print("Input file not found. Exiting.")
        return None, None, None

    file_extension = input_file_path.split('.')[-1]

    try:
        if file_extension == 'xlsx':
            df = pd.read_excel(input_file_path)
        elif file_extension == 'csv':
            df = pd.read_csv(input_file_path)
        else:
            print(f"Unsupported file format: {file_extension}")
            return None, None, None

        ingredients_column = get_column_name(df, "Ingredients Column", "Enter the name of the column containing the ingredients:")
        products_column = get_column_name(df, "Products Column", "Enter the name of the column containing the product names:")

        df['full_ingredients_list'] = df[ingredients_column]
        df['cleaned_ingredients'] = df[ingredients_column].apply(clean_ingredients)

        split_delimiters = r',\s+'
        df['cleaned_ingredients'] = df['cleaned_ingredients'].str.split(split_delimiters)

        df_exploded = df.explode('cleaned_ingredients')
        df_exploded['product_index'] = df_exploded.groupby(products_column).ngroup() + 1

        df_exploded['full_ingredients_list'] = df_exploded.groupby(products_column)['full_ingredients_list'].transform('first')
        df_exploded.loc[df_exploded.duplicated(products_column), 'full_ingredients_list'] = ''

        df_exploded['ingredient_order'] = df_exploded.groupby(products_column).cumcount() + 1
        df_exploded.reset_index(drop=True, inplace=True)
        df_exploded['ingredient_index'] = df_exploded.index + 1

        output_file = os.path.join(output_directory, f'{current_date}_processed_output.{file_extension}')
        if file_extension == 'xlsx':
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df_exploded.to_excel(writer, sheet_name='Processed Data', index=False)
        elif file_extension == 'csv':
            df_exploded.to_csv(output_file, index=False)

        return df_exploded, ingredients_column, products_column

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None, None

def find_top3_matches(value, col):
    def calculate_distance(x):
        x_str = str(x) if x is not None else ""
        value_str = str(value) if value is not None else ""
        return levenshtein_distance(value_str, x_str)

    distances = col.apply(calculate_distance)
    sorted_indexes = distances.argsort()[:3]
    return distances[sorted_indexes], sorted_indexes

def process_data(df_inci, ingredients_column, products_column):
    print("Reading additional input data...")
    profiles_pharos = pd.read_csv(pharos_profiles_path, dtype=str)
    cas_mapping = pd.read_excel(cas_mapping_path, sheet_name="mappings", engine='openpyxl', dtype=str)

    print("Preprocessing data...")
    df_inci["ingredients_lower"] = df_inci[ingredients_column].str.lower()
    profiles_pharos["name_lower"] = profiles_pharos["name"].str.lower()
    profiles_pharos["inci_lower"] = profiles_pharos["inci"].str.lower()
    cas_mapping["name_lower"] = cas_mapping["name"].str.lower()

    print("Preparing output data...")
    output_data = {}
    sheet_names = ["first_distance_matches", "second_distance_matches", "third_distance_matches"]
    for sheet_name in sheet_names:
        output_data[sheet_name] = df_inci.copy()

    for idx, ingredient in tqdm(df_inci["ingredients_lower"].items(), desc="Processing ingredients"):
        for df_name, df in [("profiles_pharos_name", profiles_pharos["name_lower"]),
                            ("profiles_pharos_inci", profiles_pharos["inci_lower"]),
                            ("cas_mapping_name", cas_mapping["name_lower"])]:
            distances, indexes = find_top3_matches(ingredient, df)
            for i, (distance, index) in enumerate(zip(distances, indexes)):
                sheet_name = sheet_names[i]
                output_data[sheet_name].loc[idx, "distance_lev"] = distance

                if df_name.startswith("profiles_pharos"):
                    matched_row = profiles_pharos.loc[index]
                    output_data[sheet_name].loc[idx, 'map_casrn'] = str(matched_row['casrn'])
                    output_data[sheet_name].loc[idx, 'map_name'] = str(matched_row['name'])
                    output_data[sheet_name].loc[idx, 'map_inci'] = str(matched_row['inci'])
                    output_data[sheet_name].loc[idx, 'map_status'] = str(matched_row['status'])
                    output_data[sheet_name].loc[idx, 'map_hazard'] = str(matched_row['hazard_band'])
                    output_data[sheet_name].loc[idx, 'map_c2c'] = str(matched_row['c2c_score'])
                    output_data[sheet_name].loc[idx, 'map_scil'] = str(matched_row['scil_status'])
                    output_data[sheet_name].loc[idx, 'map_tco'] = str(matched_row['tco_status'])
                    output_data[sheet_name].loc[idx, 'map_additional_casrn'] = str(matched_row['additional_casrn'])

                elif df_name == "cas_mapping_name":
                    matched_row = cas_mapping.loc[index]
                    casrn = str(matched_row['casrn'])
                    matching_pharos_row = profiles_pharos.loc[profiles_pharos['casrn'] == casrn]

                    if not matching_pharos_row.empty:
                        pharos_row = matching_pharos_row.iloc[0]
                        output_data[sheet_name].loc[idx, 'map_casrn'] = str(pharos_row['casrn'])
                        output_data[sheet_name].loc[idx, 'map_name'] = str(pharos_row['name'])
                        output_data[sheet_name].loc[idx, 'map_inci'] = str(pharos_row['inci'])
                        output_data[sheet_name].loc[idx, 'map_status'] = str(pharos_row['status'])
                        output_data[sheet_name].loc[idx, 'map_hazard'] = str(pharos_row['hazard_band'])
                        output_data[sheet_name].loc[idx, 'map_c2c'] = str(pharos_row['c2c_score'])
                        output_data[sheet_name].loc[idx, 'map_scil'] = str(pharos_row['scil_status'])
                        output_data[sheet_name].loc[idx, 'map_tco'] = str(pharos_row['tco_status'])
                        output_data[sheet_name].loc[idx, 'map_additional_casrn'] = str(pharos_row['additional_casrn'])
                        output_data[sheet_name].loc[idx, 'map_mappings_additional_casrn'] = str(matched_row['additional_casrn'])
                    else:
                        output_data[sheet_name].loc[idx, 'map_source'] = "mapping"
                        output_data[sheet_name].loc[idx, 'map_casrn'] = str(matched_row['casrn'])
                        output_data[sheet_name].loc[idx, 'map_name'] = str(matched_row['name'])
                        output_data[sheet_name].loc[idx, 'map_type'] = str(matched_row['type'])
                        output_data[sheet_name].loc[idx, 'map_additional_casrn'] = str(matched_row['additional_casrn'])

    print("Writing output data to output files...")
    for sheet_name, df in output_data.items():
        output_file = os.path.join(output_directory, f'{current_date}_{sheet_name}.xlsx')
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)

    print(f"Writing completed. Check the output files in {output_directory}.")

if __name__ == "__main__":
    df_inci, ingredients_column, products_column = process_inci()
    if df_inci is not None:
        process_data(df_inci, ingredients_column, products_column)
