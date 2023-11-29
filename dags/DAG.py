"""
Name: Fernaldy Aristo Wirjowerdojo

// DAG.py //
This programme was created to perform DAG tasks where (in order of execution):
1. Grabs data from the postgres container and saves it in the airflow container
2. Cleans the data and saves the cleaned data
3. Posts the cleaned data to elasticsearch
and is scheduled to be executed everyday at 1130PM UTC (0630AM UTC+7).
"""

import pandas as pd
import re
from sqlalchemy import create_engine
from sklearn.impute import KNNImputer
from elasticsearch import Elasticsearch, helpers

import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator



def connect(username: str='postgres', password: str='postgres', host: str='postgres', port: int=5432, database: str='sdg', table_name: str='table_sdg'):
    """
    Creates a connection to the postgresql database given the arguments `username`, `password` and `database`. 
    Then, it grabs the table from the database following the argument `table_name` 
    Parameters:
    - username: Username to connect to the server
    - password: Password for the above username
    - host: Host connection endpoint
    - port: Port endpoint
    - database: Name of database to connect to
    - table_name: Table name inside the database to grab
    Returns the table in a pandas dataframe format

    Example usage:
    # connect('postgres', 'postgres', 'localhost', 5432, 'sales', 'my_table')
    -> Uses the username: `postgres` with the password: `postgres` to connect to the `sales` database
    -> Grabs the table called `my_table` from the `sales` database 
    """
    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
    table = pd.read_sql_table(table_name, engine)
    table.to_csv('/opt/airflow/data_raw.csv', index=False)
    # return table


def clean_data(data_path: str):
    """
    Given the argument `data_path` that is expected to be the file path to the raw data grabbed from the first function `connect`. 
    This function reads the data (.csv) into a pandas dataframe and performs data cleaning by:
    1. Normalising the column names -> Applying lowercase, removing details, whitespaces, non alpha-numeric characters except underscore
    2. Drops the column `financial_flows_to_developing_countries` and `renewables`
    3. Imputes the missing values using KNN Imputer 
    4. Creates a csv file of the cleaned and imputed data 

    # example usage:
    clean_data('/tmp/data_raw.csv')
    """
    data = pd.read_csv(data_path)
    
    ## Normalise columns
    normalised_columns = [] # List to store the normalised column names
    for col in data.columns:
        # Convert to lowercase
        col = col.lower()
    
        # Remove details in parentheses
        col = re.sub(r'\([^)]*\)', '', col)

        # Replace non-alphanumeric characters (except underscore) with space
        col = re.sub(r'[^a-z0-9_]', ' ', col)

        # Replace spaces and hyphens with underscore and strip trailing / leading underscores
        col = re.sub(r'[\s-]+', '_', col).strip('_')

        normalised_columns.append(col)
    data.columns = normalised_columns # Replace column names with the normalised column names

    # -----------------------------------------------------------------------------------------
    ## Drop columns: Renewables and financial flows
    data.drop(columns=['financial_flows_to_developing_countries', 'renewables'], axis=1, inplace=True)

    # -----------------------------------------------------------------------------------------
    ## Fixing percentage columns (except gdp growth) to max out at 100%
    pct_cols = ['access_to_electricity', 'access_to_clean_fuels_for_cooking', 'renewable_energy_share_in_the_total_final_energy_consumption', 'low_carbon_electricity']
    for col in pct_cols:
        data.loc[data[col] > 100, col] = 100
    
    # -----------------------------------------------------------------------------------------
    # Fix error in value input on density column
    data['density_n'] = data['density_n'].str.replace(',', '.')
    data['density_n'].astype('float')

    # -----------------------------------------------------------------------------------------
    ## Impute missing values
    # Separate numerical and categorical columns // list out the numerical column names 
    numerics, numeric_cols = data.select_dtypes(include='number'), data.select_dtypes(include='number').columns.to_list()
    categoricals = data.select_dtypes(include='object')

    # Create an instance of KNNImputer to impute the numerical columns
    imputer = KNNImputer(n_neighbors=15)
    numerics_imputed = imputer.fit_transform(numerics)
    numerics_df = pd.DataFrame(numerics_imputed, columns=numeric_cols, index=numerics.index)
    
    # Merge the imputed numerical columns with the categorical columns
    data_imputed = pd.concat([categoricals, numerics_df], axis=1)

    # Imputing the final missing value due to conversion error
    data_imputed.fillna(data['density_n'].median(), inplace=True)

    # -----------------------------------------------------------------------------------------
    ## Fixing year data type
    data_imputed['year'] = data_imputed['year'].astype('int')

    # -----------------------------------------------------------------------------------------
    ## Create a csv file of the cleaned data to save locally
    data_imputed.to_csv('/opt/airflow/data_clean.csv', index=False)
    
    # return data_imputed


def post_to_elasticsearch(data_path: str, index_name: str, es_host='http://elasticsearch:9200'):
    """
    Given the arguments: `data_path`, `index_name` and `es_host`, this function fetches the cleaned data to:
    1. Convert to json format (dictionary)
    2. Creates the elasticsearch client
    3. Posts the data into elasticsearch
    """
    ## Read the csv file and convert to json format
    data = pd.read_csv(data_path)

    # ----------------------------------------------
    ## Create the Elasticsearch client
    es = Elasticsearch(hosts=es_host)

    # ----------------------------------------------
    ## Prepare data for bulk indexing
    actions = [
        {
            "_index": index_name,
            "_source": row.to_dict()
        }
        for _, row in data.iterrows()
    ]
    helpers.bulk(es, actions)


default_args = { # Default arguments for DAG tasks
    'owner': 'fernaldy',
    'start_date': dt.datetime(2023, 11, 24),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=3),
}

connect_args = { # Arguments for the `connect` function
    'username': 'postgres',
    'password': 'postgres',
    'host': 'postgres',
    'port': 5432,
    'database': 'sdg',
    'table_name': 'table_sdg'
}

clean_data_args = { # Argument for the `clean_data` function
    'data_path': '/opt/airflow/data_raw.csv'
}

elasticsearch_args = { # Arguments for the `post_to_elasticsearch` function
    'data_path': '/opt/airflow/data_clean.csv',
    'index_name': 'sdg_data'
}


with DAG('fetch_clean_elastic',
         default_args=default_args,
         schedule_interval='30 23 * * *', # Daily at 1130PM UTC // 630AM UTC+7
         catchup=False) as dag:
    
    connect_task = PythonOperator(
        task_id='connect',
        python_callable=connect,
        op_kwargs=connect_args
    )

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        op_kwargs=clean_data_args
    )

    post_to_elasticsearch_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch,
        op_kwargs=elasticsearch_args
    )

    connect_task >> clean_data_task >> post_to_elasticsearch_task