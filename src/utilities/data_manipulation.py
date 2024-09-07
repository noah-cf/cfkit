import pandas as pd

def clear_false(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace False values with empty strings in the DataFrame.
    """
    return df.replace({False: ""})

def handle_report_source_input(file_path: str, file_format: str) -> pd.DataFrame:
    """
    Load data from a file and prepend "source_" to the beginning of each column name. Also add a 'source_index' column.
    """
    if file_format == "csv":
        df = pd.read_csv(file_path)
    elif file_format == "excel":
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Invalid file format. Valid formats are CSV and Excel")

    df.columns = ["source_" + col.lower() for col in df.columns]
    df['source_index'] = range(1, len(df) + 1)
    return df


def add_columns(df, columns):
    """
    Add specified columns to a DataFrame with empty strings as default values.
    """
    for column in columns:
        df[column] = ''
    return df

def reorder_columns(df, new_order):
    """
    Reorder the columns of a DataFrame.
    """
    df = df[new_order]
    return df

def datetime_delocal_date_only(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert date and time to non-localized date only for datetime columns in the DataFrame.
    """
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)
            df[col] = df[col].dt.date
    return df

def map_column_names(df: pd.DataFrame, mapping_dict: dict) -> pd.DataFrame:
    """
    Rename columns of a DataFrame based on a given dictionary
    """
    return df.rename(columns=mapping_dict)

def map_values(df: pd.DataFrame, column: str, mapping_dict: dict) -> pd.DataFrame:
    """
    Replace values in a specific column of a DataFrame based on a given dictionary
    """
    if column in df.columns:
        df[column] = df[column].map(mapping_dict)
    return df

def apply_custom_order(df: pd.DataFrame, order: list, id_column: str) -> pd.DataFrame:
    """
    Apply a custom order to a DataFrame.
    """
    max_order = max(order) + 1
    df['Order'] = df[id_column].apply(lambda x: order.index(x) if x in order else max_order - x)
    df.sort_values('Order', inplace=True)
    df.drop('Order', axis=1, inplace=True)
    return df