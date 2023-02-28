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
from use_case_tmz.ml_logic.params import PSI_API_KEY_1



def get_data_from_both_apis(url) -> pd.DataFrame :

    print(Fore.YELLOW + f'Retrieving data for site {url}' + Style.RESET_ALL)
    key = PSI_API_KEY_1
    psi_kpis= page_speed_insight_kpis(url, key)
    sw_kpis= get_data_from_similar(url)

    print(Fore.GREEN + f'Data from APIs available for site {url}' + Style.RESET_ALL)
    both_apis_data = pd.merge(psi_kpis, sw_kpis, on='site_url', how='left')

    both_apis_data = both_apis_data.drop(columns='Top_Geo')

    return both_apis_data



def blocklist(blocklist_value = 0):
    return blocklist_value



def ads_format_selected(url, _1_=0, _2_=1, _3_=0, _4_=0, _5_=0,
       _6_=0, _11_=0, _15_=0, _16_=0, _19_=0, _20_=0, _24_=0, _27_=0, _28_=0, _30_=0,
       _31_=0, _34_=0, _38_=0, _39_=0, _43_=0, _44_=0, _46_=0):

    format_dict = {
        'site_url':url,
        '_1_':_1_,
        '_2_':_2_,
        '_3_':_3_,
        '_4_':_4_,
        '_5_':_5_,
        '_6_':_6_,
        '_11_':_11_,
        '_15_':_15_,
        '_16_':_16_,
        '_19_':_19_,
        '_20_':_20_,
        '_24_':_24_,
        '_27_':_27_,
        '_28_':_28_,
        '_30_':_30_,
        '_31_':_31_,
        '_34_':_34_,
        '_38_':_38_,
        '_39_':_39_,
        '_43_':_43_,
        '_44_':_44_,
        '_46_':_46_
    }

    return pd.DataFrame(format_dict, index=[0])




def number_of_formats(df):
    num_values = df.select_dtypes(include='int')
    return num_values.values.sum()





def top_geo(geo=np.NaN):
    return geo





def all_data_for_one_site(url, blocklist_value, geo, _1_=0, _2_=1, _3_=0, _4_=0, _5_=0,
       _6_=0, _11_=0, _15_=0, _16_=0, _19_=0, _20_=0, _24_=0, _27_=0, _28_=0, _30_=0,
       _31_=0, _34_=0, _38_=0, _39_=0, _43_=0, _44_=0, _46_=0):

    df = get_data_from_both_apis(url)

    df['blocklist_value'] = blocklist(blocklist_value)

    df['geo'] = top_geo(geo)

    df_format = ads_format_selected(url, _1_, _2_, _3_, _4_, _5_,
       _6_, _11_, _15_, _16_, _19_, _20_, _24_, _27_, _28_, _30_,
       _31_, _34_, _38_, _39_, _43_, _44_, _46_)

    df['nb_formats'] = number_of_formats(df_format)

    df = pd.merge(df, df_format, on='site_url', how='left')

    return df




if __name__ == '__main__':
    # get_data_from_both_apis('https://dailyloannews.com')
    # ads_format_selected('https://dailyloannews.com', format_44=1, format_11=1)
    df_test = all_data_for_one_site('https://www.coursfrancaisfacile.com', 1, 'US', _19_=1, _46_=1)
    print(df_test.columns)
    # print(number_of_formats(ads_format_selected('https://dailyloannews.com', format_44=1, format_11=1)))
