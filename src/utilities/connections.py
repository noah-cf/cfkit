import json
import os
import sqlalchemy
import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from oauth2client.service_account import ServiceAccountCredentials
import gspread

def load_config():
    config_file_path = os.path.join(os.path.dirname(__file__), '../../assets/config.json')
    with open(config_file_path, 'r') as config_file:
        return json.load(config_file)

config = load_config()

class Config:
    DB_PROD = config['database']['prod']
    DB_STG = config['database']['stg']
    GOOGLE_CLOUD_KEY = os.path.join(os.path.dirname(__file__), '../../assets', config['google']['json_keyfile'])

def get_db_engine(environment='prod'):
    """
    Create a SQLAlchemy engine based on the specified environment.
    
    :param environment: Target database environment ('prod' or 'stg').
    :return: SQLAlchemy engine.
    """
    db_config = Config.DB_PROD if environment == 'prod' else Config.DB_STG
    connection_string = f"{db_config['type']}://{db_config['user']}:" \
                        f"{db_config['password']}@{db_config['host']}/{db_config['name']}"
    return sqlalchemy.create_engine(connection_string)

@contextmanager
def db_session(engine: Engine):
    """Provide a transactional scope around a series of operations."""
    connection = engine.connect()
    transaction = connection.begin()
    try:
        yield connection
        transaction.commit()
    except SQLAlchemyError as e:
        transaction.rollback()
        raise e
    finally:
        connection.close()

def run_query(engine: Engine, query: str) -> pd.DataFrame:
    """
    Execute a SQL query using the provided database engine and return the results as a DataFrame.

    :param engine: SQLAlchemy engine instance.
    :param query: SQL query string.
    :return: DataFrame containing the query results.
    """
    with engine.connect() as connection:
        return pd.read_sql(query, connection)

def get_google_sheets_client() -> gspread.Client:
    """
    Authenticates with Google Sheets using the JSON keyfile specified in the configuration and returns the client object.

    :return: gspread.Client object
    """
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(Config.GOOGLE_CLOUD_KEY, scope)
    client = gspread.authorize(creds)
    return client

# Example usage:
if __name__ == "__main__":
    engine = get_db_engine('prod')
    query = "SELECT * FROM your_table"
    
    result_df = run_query(engine, query)
    print(result_df)
    
    client = get_google_sheets_client()
    spreadsheet = client.open("Your Spreadsheet Name")
    print(spreadsheet.title)
