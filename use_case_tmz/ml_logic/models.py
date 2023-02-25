import joblib
import os


def load_model(file_name):

    root = os.path.dirname(os.path.dirname(__file__))

    file_to_download =f'{root}/saved_models/{file_name}.joblib'

    loaded_model = joblib.load(file_to_download)

    return loaded_model





if __name__=="__main__":
    print(load_model('stacking_model'))
