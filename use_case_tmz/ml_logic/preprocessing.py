from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.pipeline import Pipeline, make_pipeline, FeatureUnion
from sklearn.compose import ColumnTransformer, make_column_transformer, make_column_selector
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import RobustScaler, OneHotEncoder, OrdinalEncoder
from sklearn.metrics import make_scorer
from sklearn.model_selection import cross_val_score
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import Ridge, Lasso, LinearRegression
from sklearn.model_selection import RandomizedSearchCV
from sklearn.svm import SVR
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.ensemble import AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import VotingRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import StackingRegressor
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import pandas as pd
from use_case_tmz.ml_logic.data_preparation import all_data_for_one_site


def preprocess_features(X: pd.DataFrame):

    # Category column cleaned
    Cat_final = ['news and media','science and education','community and society','e-commerce and shopping','jobs and career','arts and entertainment','games',
             'finance','lifestyle','heavy industry and engineering','computers electronics and technology','food and drink','law and government','pets and animals','adult',
             'sports','gambling','business and consumer services','reference materials','travel and tourism','home and garden','hobbies and leisure','health','vehicles']

    def clean_category(cat):
        if cat == "nan":
            return np.NAN
        else:
            cat1= cat.replace("_", " ")
            cat2= cat1.split('>')
            cat3= cat2[0].split('/')
            cat4= cat3[0].replace("&", "and").strip()
        if cat4 not in Cat_final:
            return np.NAN
        else:
            return cat4

    X['Category'] = X['Category'].apply(lambda x: clean_category(str(x)))


    # Determine geo if one of top 20 or need to be put in others category
    geo_final = ['FR',
                'JP',
                'IT',
                'BR',
                'ES',
                'DE',
                'US',
                'RU',
                'MX',
                'IN',
                'AR',
                'CO',
                'UA',
                'GB',
                'ID',
                'PE',
                'GR',
                'CL',
                'KZ',
                'NG']
    def clean_top_geo(geo):
        if geo == "nan":
            return 'Other'
        else:
            if geo not in geo_final:
                return 'Other'
            else:
                return geo

    X['geo']=X['geo'].apply(lambda x: clean_top_geo(str(x)))



    # def clean_format(format_pub):
    #     if format_pub == "nan":
    #         return 0
    #     else:
    #         return 1

    # for column in ['_1', '_2', '_3', '_4', '_5', '_6', '_11','_15', '_16', '_19', '_20', '_24', '_27', '_28', '_30', '_31', '_34','_38', '_39', '_43', '_44', '_46'] :
    #     X[f"{column}_"]=X[column].apply(lambda x: clean_format(str(x)))

    # Preprocess KPIs from Similarweb
    X = X.drop(columns='Paid_Referrals')
    X['Social'] = X['Social'].fillna(0)
    X['Mail'] = X['Mail'].fillna(0)
    X['Referrals'] = X['Referrals'].fillna(0)
    X['Direct'] = X['Direct'].fillna(0)
    X['Search'] = X['Search'].fillna(0)

    # y = X['ca_journalier']
    # X = X.drop(columns=['site_url','site_category_similar','Category','site_id','site_url','_1', '_2', '_3', '_4', '_5', '_6', '_11','_15', '_16', '_19', '_20', '_24', '_27', '_28', '_30', '_31', '_34','_38', '_39', '_43', '_44', '_46'],axis=1)

    # # Preprocess numerical columns
    # num_cols = X.select_dtypes(include=["int64", "float64"]).columns.to_list()

    # for i in ['_1_', '_2_', '_3_', '_4_', '_5_', '_6_', '_11_','_15_', '_16_', '_19_', '_20_', '_24_', '_27_', '_28_', '_30_', '_31_', '_34_','_38_', '_39_', '_43_', '_44_', '_46_'] :
    #     num_cols.remove(i)

    # # Preprocess category columns
    # cat_cols = X.select_dtypes(exclude=["int64", "float64"]).columns.to_list()

    # # feat_numerical = sorted(X.select_dtypes(include=["int64", "float64"]).columns)

    # # Numerical pipeline excluding formats
    # num_pipeline = Pipeline(steps=[
    #     ('impute', SimpleImputer(strategy='mean')),
    #     ('scale',StandardScaler())
    # ])


    # # feat_nominal = sorted(list(set(X.columns) - set(feat_numerical)))

    # # Categorical pipeline
    # cat_pipeline = Pipeline(steps=[
    #     ('impute', SimpleImputer(strategy='most_frequent')),
    #     ('one-hot',OneHotEncoder(handle_unknown='ignore', sparse=False))
    # ])

    # # Joining both pipelines
    # col_trans = ColumnTransformer(transformers=[
    # ('num_pipeline',num_pipeline,num_cols),
    # ('cat_pipeline',cat_pipeline,cat_cols)
    # ],
    # remainder='passthrough',
    # n_jobs=-1)

    # X_preproc = pd.DataFrame(col_trans.transform(X),columns=col_trans.get_feature_names_out())

    X = X.drop(columns='site_url')
    X = X.rename(columns={'Category': 'New_Site_Categ',
                          'blocklist_value': 'site_blocklist',
                          'geo': 'top_geo_code_country',
                          'nb_formats': 'nb_dis_format_d_mean'})

    X = X[['lighthouse_score', 'LCP', 'FID', 'CLS', 'FCP', 'INP', 'TTFB', 'Social',
       'Mail', 'Referrals', 'Search', 'Direct', 'BounceRate', 'PagePerVisit',
       'EstimatedMonthlyVisits', 'site_blocklist', 'top_geo_code_country',
       'nb_dis_format_d_mean', 'New_Site_Categ', '_1_', '_2_', '_3_', '_4_',
       '_5_', '_6_', '_11_', '_15_', '_16_', '_19_', '_20_', '_24_', '_27_',
       '_28_', '_30_', '_31_', '_34_', '_38_', '_39_', '_43_', '_44_', '_46_']]

    return X






if __name__ == '__main__':
    df_test = all_data_for_one_site('https://www.coursfrancaisfacile.com', 1, 'US', _19_=1, _46_=1)
    print(preprocess_features(df_test).columns)
