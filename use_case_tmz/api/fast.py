from fastapi import FastAPI
from use_case_tmz.ml_logic.models import load_model

app = FastAPI()
# app.state.model = load_model()

@app.get("/")
def root():
    return {'greeting': 'Welcome to our project'}
