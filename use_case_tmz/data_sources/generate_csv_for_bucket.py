import requests
import pandas as pd

from google.cloud import bigquery
from use_case_tmz.ml_logic.params import PSI_API_KEY, PROJECT, DATASET, TABLE, TABLE_TO, PSI_API_KEY_1, PSI_API_KEY_2, PSI_API_KEY_3

from numpy import NaN

from colorama import Fore, Style

from pathlib import Path

import os
import math

def data_from_bq():
    for i in range(101):
        table =f"{PROJECT}.{DATASET}.{TABLE}"
        client = bigquery.Client()

        rows = client.list_rows(table,
                                start_index=(i * 100),
                                max_results=100)
        big_query_df = rows.to_dataframe().rename({'site_url': 'url'}, axis=1)
        site_url_df = big_query_df['url']

        # filepath = Path(f'../../data/raw_data/{i}.csv')
        # filepath.parent.mkdir(parents=True, exist_ok=True)
        root = os.path.dirname(os.path.dirname(__file__))
        site_url_df.to_csv(f'{root}/data/raw_data/{i}.csv')
        print(f'{root}/data/raw_data/{i}.csv')

def data_from_csv(file_name, folder_name):
    # Localize & read csv
    root = os.path.dirname(os.path.dirname(__file__))
    path = f'{root}/data/raw_data/rest'
    df = pd.read_csv(f'{path}/{file_name}.csv').rename({'site_url': 'url'}, axis=1)
    df = df['url']

    # Generate nb of necessary files
    e = 0
    index = 0
    for i in range(0, len(df), 100):
        df.iloc[e:i].to_csv(f"{path}/{folder_name}/{index}.csv")
        e = i
        print(f'{index} csv done')
        index += 1





if __name__=="__main__":
    data_from_csv('20230219_1227', '3rd_round')
