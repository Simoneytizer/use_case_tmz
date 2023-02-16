import requests
import pandas as pd

from google.cloud import bigquery
from use_case_tmz.ml_logic.params import PSI_API_KEY, PROJECT, DATASET, TABLE, TABLE_TO, PSI_API_KEY_1, PSI_API_KEY_2, PSI_API_KEY_3

from numpy import NaN

from colorama import Fore, Style

from pathlib import Path

import os

for i in range(51):
    table =f"{PROJECT}.{DATASET}.{TABLE}"
    client = bigquery.Client()

    rows = client.list_rows(table,
                            start_index=(i * 200),
                            max_results=200)
    big_query_df = rows.to_dataframe().rename({'site_url': 'url'}, axis=1)
    site_url_df = big_query_df['url']

    # filepath = Path(f'../../data/raw_data/{i}.csv')
    # filepath.parent.mkdir(parents=True, exist_ok=True)
    root = os.path.dirname(os.path.dirname(__file__))
    site_url_df.to_csv(f'{root}/data/raw_data/{i}.csv')
    print(f'{root}/data/raw_data/{i}.csv')
