import pandas as pd
import os
from use_case_tmz.ml_logic.params import PROJECT, DATASET, TABLE_10k_SS, TABLE, TABLE_10k_AM, TABLE_DATA_API
from google.cloud import bigquery
import sys
import subprocess
import math
import requests

"""
1. Save *._enriched.csv to bigquery
2. Load BigQuery table (the one created with api results)
3. Find rest
4. Rest through API
5. Until len(find_rest) == 0
"""

# Function to get all csv done by reketator into one dataframe
def reketator_result():

    df_all = pd.read_csv('gs://wagonxmoneytizer/0._enriched.csv', index_col=0)

    for i in range(1, 101):
        try:
            df = pd.read_csv('gs://wagonxmoneytizer/%s' % (str(i)+"._enriched.csv", ), index_col=0)
            df_all = pd.concat([df_all, df])
        except:
            pass

    nb_url_reketator = df_all['site_url'].shape[0]

    print(f'{nb_url_reketator} urls done by reketator')
    return df_all


# Function to get the data from Sylvie table done locally
def sylvie_result():

    table =f"{PROJECT}.{DATASET}.{TABLE_10k_SS}"
    client = bigquery.Client()

    rows = client.list_rows(table,
                        start_index=0)
    big_query_df = rows.to_dataframe()

    nb_url_sylvie = big_query_df['site_url'].shape[0]

    print(f'{nb_url_sylvie} urls done by Sylvie')
    return big_query_df

# Merging both url sources
def merging_result():

    df_all = reketator_result()
    big_query_df = sylvie_result()

    df_all_bq = pd.concat([df_all, big_query_df])
    df_all_bq.drop_duplicates(subset='site_url',inplace=True)

    df_all_bq['Top_Geo'] = df_all_bq['Top_Geo'].astype('float64')

    nb_url_all = df_all_bq['site_url'].shape[0]

    print(f'{nb_url_all} urls in total')
    return df_all_bq

# Checking which urls are missing and returning into a dataframe
def find_rest()-> pd.DataFrame:

    # save_to_bq(merging_result())

    # Call the initial table to compare & find the missing urls
    table_initial =f"{PROJECT}.{DATASET}.{TABLE}"
    client = bigquery.Client()

    rows_initial = client.list_rows(table_initial,
                            start_index=0)

    df_url_initial = rows_initial.to_dataframe()

    nb_url_initial = df_url_initial['site_url'].shape[0]

    print(f'{nb_url_initial} urls in total in the original database')

    # Compare with urls already done
    table_api_result =f"{PROJECT}.{DATASET}.{TABLE_DATA_API}"

    rows_api_result = client.list_rows(table_api_result,
                        start_index=0)

    df_api_result = rows_api_result.to_dataframe()

    test = [False if x in df_api_result['site_url'].values else True for x in df_url_initial['site_url'].values]
    df_rest = df_url_initial[test]

    nb_url_rest = df_rest['site_url'].shape[0]

    print(f'{nb_url_rest} remaining urls to go through API')
    return df_rest

def rest_through_api():
    df_rest = find_rest().rename(columns={'site_url': 'url'})



    # Loop over same as before to split into 100 lines csv and send to bucket
    print(len(df_rest))
    e = 0
    index = 0
    for i in range(0, len(df_rest), math.ceil(len(df_rest)/100)):
        df_rest.iloc[e:i].to_csv(f"gs://wagonxmoneytizer/{index}.csv")
        e = i
        # API request to launch
        # url = f'https://reketor-ut5nyuuria-ew.a.run.app/from_gcs_to_gcs?bucket_name=wagonxmoneytizer&blob_name={i}.csv'
        # response = requests.get(url)
        print(subprocess.run(['curl "https://reketor-ut5nyuuria-ew.a.run.app/from_gcs_to_gcs?bucket_name=wagonxmoneytizer&blob_name=%s.csv"' % index],shell=True, capture_output=True))
        index += 1

def new_data(df):

    table_api_result =f"{PROJECT}.{DATASET}.{TABLE_DATA_API}"
    client = bigquery.Client()

    rows_api_result = client.list_rows(table_api_result,
                        start_index=0)

    df_api_result = rows_api_result.to_dataframe()

    filter_new_data = [False if x in df_api_result['site_url'].values else True for x in df['site_url'].values]
    df_new_data = df[filter_new_data]

    nb_url_new_data = df_new_data['site_url'].shape[0]
    print(f'{nb_url_new_data} done')

    return df_new_data



def save_to_bq(df):
    table_to_send_to = f"{PROJECT}.{DATASET}.{TABLE_DATA_API}"
    client = bigquery.Client()

    write_mode = "WRITE_APPEND"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode,
                                        schema = [
                                            bigquery.SchemaField('site_url', 'STRING'),
                                            bigquery.SchemaField('lighthouse_score', 'FLOAT'),
                                            bigquery.SchemaField('LCP', 'FLOAT'),
                                            bigquery.SchemaField('FID', 'INTEGER'),
                                            bigquery.SchemaField('CLS', 'FLOAT'),
                                            bigquery.SchemaField('FCP', 'FLOAT'),
                                            bigquery.SchemaField('INP', 'INTEGER'),
                                            bigquery.SchemaField('TTFB', 'FLOAT'),
                                            bigquery.SchemaField('Social', 'FLOAT'),
                                            bigquery.SchemaField('Paid_Referrals', 'FLOAT'),
                                            bigquery.SchemaField('Mail', 'FLOAT'),
                                            bigquery.SchemaField('Referrals', 'FLOAT'),
                                            bigquery.SchemaField('Search', 'FLOAT'),
                                            bigquery.SchemaField('Direct', 'FLOAT'),
                                            bigquery.SchemaField('BounceRate', 'FLOAT'),
                                            bigquery.SchemaField('PagePerVisit', 'FLOAT'),
                                            bigquery.SchemaField('Category', 'STRING'),
                                            bigquery.SchemaField('EstimatedMonthlyVisits', 'INTEGER'),
                                            bigquery.SchemaField('Top_Geo', 'FLOAT')
                                        ]
                                        )

    job = client.load_table_from_dataframe(df, table_to_send_to, job_config=job_config)
    result = job.result()
    print('Loaded to BigQuery')

def save_to_csv(df, file_name):
    root = os.path.dirname(os.path.dirname(__file__))
    df.to_csv(f'{root}/data/raw_data/rest/{file_name}.csv')
    print(f'Saved in csv under {root}/data/raw_data/rest/{file_name}.csv')

if __name__=="__main__":
    # args = sys.argv
    # globals()[args[1]](*args[2:])

    # while len(find_rest())!= 0:
    #     rest_through_api()

    # save_to_bq(merging_result())
    save_to_csv(find_rest())
    # print(len(reketator_result()))
    # save_to_bq(new_data(merging_result()))
