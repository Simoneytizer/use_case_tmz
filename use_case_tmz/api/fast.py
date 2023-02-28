from fastapi import FastAPI

from use_case_tmz.ml_logic.data_preparation import all_data_for_one_site
from use_case_tmz.ml_logic.preprocessing import preprocess_features
from use_case_tmz.ml_logic.models import load_pipe_ensemble, predict_value

app = FastAPI()
app.state.model = load_pipe_ensemble('pipe_ensemble_model')

@app.get("/")
def root():
    return {'greeting': 'Welcome to our project'}



@app.get("/predict")
def predict(site_url, blocklist_value=0, geo='Other', _1_=0, _2_=1, _3_=0, _4_=0, _5_=0,
       _6_=0, _11_=0, _15_=0, _16_=0, _19_=0, _20_=0, _24_=0, _27_=0, _28_=0, _30_=0,
       _31_=0, _34_=0, _38_=0, _39_=0, _43_=0, _44_=0, _46_=0):

    X = all_data_for_one_site(site_url, blocklist_value, geo, _1_, _2_, _3_, _4_, _5_,
       _6_, _11_, _15_, _16_, _19_, _20_, _24_, _27_, _28_, _30_,
       _31_, _34_, _38_, _39_, _43_, _44_, _46_)

    X_preproc = preprocess_features(X)

    model = load_pipe_ensemble('pipe_ensemble_model')

    y_pred = predict_value(X_preproc, model)

    X = X.fillna('Not found')

    return {'site_url': site_url,
            'lighthouse_score': float(X['lighthouse_score'].values[0]) if type(X['lighthouse_score'].values[0]) != str else 'Not found',
            'LCP': float(X['LCP'].values[0]) if type(X['LCP'].values[0]) != str else 'Not found',
            'FID': float(X['FID'].values[0]) if type(X['FID'].values[0]) != str else 'Not found',
            'CLS': float(X['CLS'].values[0]) if type(X['CLS'].values[0]) != str else 'Not found',
            'FCP': float(X['FCP'].values[0]) if type(X['FCP'].values[0]) != str else 'Not found',
            'INP': float(X['INP'].values[0]) if type(X['INP'].values[0]) != str else 'Not found',
            'TTFB': float(X['TTFB'].values[0]) if type(X['TTFB'].values[0]) != str else 'Not found',
            'Social': float(X['Social'].values[0]) if type(X['Social'].values[0]) != str else 'Not found',
            'Mail': float(X['Mail'].values[0]) if type(X['Mail'].values[0]) != str else 'Not found',
            'Referrals': float(X['Referrals'].values[0]) if type(X['Referrals'].values[0]) != str else 'Not found',
            'Search': float(X['Search'].values[0]) if type(X['Search'].values[0]) != str else 'Not found',
            'Direct': float(X['Direct'].values[0]) if type(X['Direct'].values[0]) != str else 'Not found',
            'BounceRate': float(X['BounceRate'].values[0]) if type(X['BounceRate'].values[0]) != str else 'Not found',
            'PagePerVisit': float(X['PagePerVisit'].values[0]) if type(X['PagePerVisit'].values[0]) != str else 'Not found',
            'Category': str(X['Category'].values[0]),
            'EstimatedMonthlyVisits': float(X['EstimatedMonthlyVisits'].values[0]) if type(X['EstimatedMonthlyVisits'].values[0]) != str else 'Not found',
            'estimated_ca': float(y_pred)}


# if __name__=="__main__":
#     print(predict('https://www.larousse.fr'))
