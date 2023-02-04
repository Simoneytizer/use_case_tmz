from google.cloud import bigquery

import pandas as pd

from use_case_tmz.ml_logic.params import PROJECT, DATASET, TABLE

def get_bq_data() -> pd.DataFrame:
    """
    return bigquery data into a dataframe
    """

    table =f"{PROJECT}.{DATASET}.{TABLE}"

    client = bigquery.Client()

    rows = client.list_rows(table)
    big_query_df = rows.to_dataframe()

    if big_query_df.shape[0] == 0:
        print(f'\nNo data to return')

    print(big_query_df.head())
    # return big_query_df

if __name__ == '__main__':
    get_bq_data()
