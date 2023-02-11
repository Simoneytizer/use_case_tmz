import requests
import pandas as pd

from google.cloud import bigquery
from use_case_tmz.ml_logic.params import PSI_API_KEY, PROJECT, DATASET, TABLE, TABLE_TO

# Function to retrieve data from page speed insights API
def page_speed_insight_kpis(site):

    # API key. To change if out
    key = f'{PSI_API_KEY}'

    # API request
    url_api = f'https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url={site}&key={key}'

    response = requests.get(url_api)
    data = response.json()

    # Get lighthouse_score available for all request
    lighthouse_score = data['lighthouseResult']['categories']['performance']['score']

    #Get additional metrics if available
    if 'metrics' in data['loadingExperience'].keys():
        LCP = data['loadingExperience']['metrics']['LARGEST_CONTENTFUL_PAINT_MS']['percentile']/1000
        FID = data['loadingExperience']['metrics']['FIRST_INPUT_DELAY_MS']['percentile']
        CLS = data['loadingExperience']['metrics']['CUMULATIVE_LAYOUT_SHIFT_SCORE']['percentile']/100
        FCP = data['loadingExperience']['metrics']['FIRST_CONTENTFUL_PAINT_MS']['percentile']/1000
        INP = data['loadingExperience']['metrics']['EXPERIMENTAL_INTERACTION_TO_NEXT_PAINT']['percentile']
        TTFB = data['loadingExperience']['metrics']['EXPERIMENTAL_TIME_TO_FIRST_BYTE']['percentile']/1000

    else:
        LCP = 'NA'
        FID = 'NA'
        CLS = 'NA'
        FCP = 'NA'
        INP = 'NA'
        TTFB = 'NA'

    # KPIs needed stored in a dict to create a DataFrame
    dict_kpis = {
        'site_url': [site],
        'lighthouse_score' : [lighthouse_score],
        'LCP' : [LCP],
        'FID' : [FID],
        'CLS' : [CLS],
        'FCP' : [FCP],
        'INP' : [INP],
        'TTFB' : [TTFB]
    }

    return pd.DataFrame(dict_kpis)

    # print(pd.DataFrame(dict_kpis))


# Function to enrich the data, take from BigQuery and return to another BigQuery table
def enrich_data_with_psi_api():

    # Get BigQuery Data
    table =f"{PROJECT}.{DATASET}.{TABLE}"
    client = bigquery.Client()

    rows = client.list_rows(table,
                            start_index=0,
                            max_results=10)
    big_query_df = rows.to_dataframe()
    site_url_df = big_query_df['site_url']

    # Parameters to send to BigQuery
    table_to_send_to = f"{PROJECT}.{DATASET}.{TABLE_TO}"
    write_mode = "WRITE_APPEND"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode)

    # Apply the page speed insight function to get KPIs for all site url
    for i in range(5):
        row_to_add = page_speed_insight_kpis(site_url_df.iloc[i])
        job = client.load_table_from_dataframe(row_to_add, table_to_send_to, job_config=job_config)
        result = job.result()
        print(f"Site_url {i} loaded to BigQuery")

if __name__ == '__main__':
    # page_speed_insight_kpis('https://reunionsaveurs.com')
    enrich_data_with_psi_api()
