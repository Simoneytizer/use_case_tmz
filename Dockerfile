FROM python:3.10.6-buster

COPY use_case_tmz /use_case_tmz
COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD uvicorn use_case_tmz.api.fast:app --host 0.0.0.0 --port $PORT
