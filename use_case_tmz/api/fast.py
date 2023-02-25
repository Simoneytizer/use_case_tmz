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
            'lighthouse_score': X['lighthouse_score'].values[0],
            'LCP': X['LCP'][0],
            'FID': X['FID'][0],
            'CLS': X['CLS'][0],
            'FCP': X['FCP'][0],
            'INP': X['INP'][0],
            'TTFB': X['TTFB'][0],
            'Social': X['Social'].values[0],
            'Mail': X['Mail'].values[0],
            'Referrals': X['Referrals'].values[0],
            'Search': X['Search'].values[0],
            'Direct': X['Direct'].values[0],
            'BounceRate': X['BounceRate'].values[0],
            'PagePerVisit': X['PagePerVisit'].values[0],
            'Category': X['Category'].values[0],
            'EstimatedMonthlyVisits': X['EstimatedMonthlyVisits'].values[0],
            'estimated_ca': y_pred}


if __name__=="__main__":
    print(predict('https://delife.fun'))
