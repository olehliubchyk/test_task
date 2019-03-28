import pickle
import psycopg2
import pandas as pd
from custom_exceptions import AppNotFoundException
 
from settings import (
    DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
    )
 
 
def select_params_from_db(app_id):
    sql = (
    """
    SELECT
    amount, durationindays, checkfield1, checkfield2, checkfield3,
    log_prevamount, freeamount, diff_dates_in_days 
    FROM public.finaldataset 
    where finaldataset.application_id =%s
    """,(app_id,))
    con = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    df = pd.read_sql(sql, con)
    if not df:
        return AppNotFoundException('Application not Found')
    print(df)
    return df
 
 
def call_model(params):
    pkl_model_name = 'c:/score/test_model.pkl'
    with open(pkl_model_name, 'rb') as file:
        pickle_model = pickle.load(file)
        score = pickle_model.predict_proba(params)
        print(score)
        return score
 
 
def get_score(app_id):
    params = select_params_from_db(app_id)
    score = call_model(params)
    return score
