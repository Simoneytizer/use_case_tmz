from google.cloud import bigquery

import pandas as pd

from colorama import Fore, Style

from use_case_tmz.ml_logic.params import PROJECT, DATASET, TABLE

def get_bq_data(index:int,
                chunk_size:int,
                verbose=True) -> pd.DataFrame:
    """
    return bigquery data into a dataframe
    """
    if verbose:
        print(Fore.MAGENTA + f"Data from bigquery: {chunk_size if chunk_size is not None else 'all'} rows (from now {index})" + Style.RESET_ALL)

    table =f"{PROJECT}.{DATASET}.{TABLE}"

    client = bigquery.Client()

    if verbose:
        if chunk_size is None:
            print(f"\nQuery {table} whole content...")
        else:
            print(f"\nQuery {table} chunk {index // chunk_size} "
                + f"([{index}-{index + chunk_size - 1}])...")

    rows = client.list_rows(table,
                            start_index=index,
                            max_results=chunk_size)

    big_query_df = rows.to_dataframe()

    if big_query_df.shape[0] == 0:
        print(f'\nNo data to return')

    print(big_query_df.head())
    # return big_query_df

if __name__ == '__main__':
    get_bq_data(index=0, chunk_size=1000)
