import joblib
import os
import numpy as np

from use_case_tmz.ml_logic.data_preparation import all_data_for_one_site
from use_case_tmz.ml_logic.preprocessing import preprocess_features

def load_pipe_ensemble(file_name):

    root = os.path.dirname(os.path.dirname(__file__))

    file_to_download =f'{root}/saved_models/{file_name}.joblib'

    loaded_model = joblib.load(file_to_download)

    return loaded_model



def predict_value(X_preproc, pipe_model):
    y_pred = pipe_model.predict(X_preproc)

    return np.exp(y_pred)[0]






if __name__=="__main__":
    # print(load_pipe_ensemble('pipe_ensemble_model'))
    df_test = all_data_for_one_site('https://www.coursfrancaisfacile.com', 1, 'US', _19_=1, _46_=1)
    X_preproc = preprocess_features(df_test)
    model = load_pipe_ensemble('pipe_ensemble_model')
    print(predict_value(X_preproc, model))
