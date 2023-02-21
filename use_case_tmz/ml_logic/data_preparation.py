"""
Get the data when evaluating an url. Need:
- All data from API -> drop top_geo
- site_blocklist -> en entrée : 0 ou 1, par défaut 0
- top_geo_code_country -> à rentrer à la mano, prévoir un menu déroulant avec plusieurs possibilités.
- Nb de format où la pub sera sur le site -> prévoir de quoi cocher pour savoir quel format pris. Soit 0, soit 1 -> 22 formats possibles.
- Somme de tous les formats à 1 -> rempli précédemment, peut donc faire une fonction pour l'obtenir

A la fin, doit avoir un dataframe avec:
df3_2 = df3_2[['site_url', 'lighthouse_score', 'LCP', 'FID', 'CLS', 'FCP', 'INP', 'TTFB', 'Social', 'Mail', 'Referrals', 'Search', 'Direct', 'BounceRate', 'PagePerVisit', 'Category',
       'EstimatedMonthlyVisits', 'site_blocklist', 'top_geo_code_country', 'ca_journalier', 'nb_dis_format_d_mean','_1', '_2', '_3', '_4', '_5',
       '_6', '_11', '_15', '_16', '_19', '_20', '_24', '_27', '_28', '_30',
       '_31', '_34', '_38', '_39', '_43', '_44', '_46']]
J'ai retiré Top_Geo (vient de l'API mais très mal renseigné on est resté sur l'ancienne var top_geo_code_country),
vu_similar (car on a EstimatedMonthlyVisits) et site_category_similar (car moins bien rempli que la nouvelle Category)
"""
import pandas as pd
import numpy as np
from colorama import Fore, Style

from use_case_tmz.data_enrichment.api_page_speed_insights import page_speed_insight_kpis, get_data_from_similar
from use_case_tmz.ml_logic.params import PSI_API_KEY

def get_data_from_both_apis(url) -> pd.DataFrame :

    print(Fore.YELLOW + f'Retrieving data for site {url}' + Style.RESET_ALL)
    key = PSI_API_KEY
    psi_kpis= page_speed_insight_kpis(url, key)
    sw_kpis= get_data_from_similar(url)

    print(Fore.GREEN + f'Data from APIs available for site {url}' + Style.RESET_ALL)
    both_apis_data = pd.merge(psi_kpis, sw_kpis, on='site_url', how='left')

    print(both_apis_data.drop(columns='Top_Geo'))
    return both_apis_data.drop(columns='Top_Geo')

def blocklist(blocklist_value = 0):
    return blocklist_value

def ads_format_selected(format_1=0, format_2=0, format_3=0, format_4=0, format_5=0,
       format_6=0, format_11=0, format_15=0, format_16=0, format_19=0, format_20=0, format_24=0, format_27=0, format_28=0, format_30=0,
       format_31=0, format_34=0, format_38=0, format_39=0, format_43=0, format_44=0, format_46=0):

    format_dict = {
        '_1':format_1,
        '_2':format_2,
        '_3':format_3,
        '_4':format_4,
        '_5':format_5,
        '_6':format_6,
        '_11':format_11,
        '_15':format_15,
        '_16':format_16,
        '_19':format_19,
        '_20':format_20,
        '_24':format_24,
        '_27':format_27,
        '_28':format_28,
        '_30':format_30,
        '_31':format_31,
        '_34':format_34,
        '_38':format_38,
        '_39':format_39,
        '_43':format_43,
        '_44':format_44,
        '_46':format_46
    }

    print(pd.DataFrame(format_dict, index=[0]))


def top_geo(geo=np.NaN):
    return geo


def all_data_for_one_site(url, blocklist_value, geo):
    df = get_data_from_both_apis(url)
    df['blocklist_value'] = blocklist(blocklist_value)
    df['geo'] = top_geo(geo)

    print(df)
    return df




if __name__ == '__main__':
    # get_data_from_both_apis('https://dailyloannews.com')
    # ads_format_selected(format_44=1, format_11=1)
    all_data_for_one_site('https://dailyloannews.com', 1, 'US')
