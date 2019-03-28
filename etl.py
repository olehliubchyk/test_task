import psycopg2
import numpy as np
from psycopg2 import Error
from settings import (
    DB_HOST, DB_NAME, DB_USER,
    DB_PASSWORD, DB_PORT
)


def get_connection():
    try:
        conn = psycopg2.connect(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)


def dictfetchall(cursor):
    # "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def execute_sql(sql, commit=True):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        if commit:
            cursor.commit()


def retrieve_data_from_db(sql):
    with get_connection().cursor() as c:
        c.execute(sql)
        rows = dictfetchall(c)
        return rows


def create_table():
    execute_sql(
        """
        CREATE TABLE IF NOT EXISTS rawdata
        ( num integer, applicationid integer, amount integer,
        durationindays TEXT, checkfield1 BOOLEAN,
        checkfield2 BOOLEAN, checkfield3 BOOLEAN,
        earned integer,spendings integer,
        prevamount integer,startdate TEXT,finishdate TEXT);
        """
    )


def load_data_from_file(cursor):
    with open('c:/calcscore/raw_data.csv', 'r') as f:
        next(f)
        cursor.copy_from(f, 'rawdata', sep=',')
        return cursor


def create_final_dataset_table():
    execute_sql(
        """
        CREATE TABLE finaldataset
        (application_id integer, amount numeric,
        durationindays numeric, checkfield1 BOOLEAN,
        checkfield2 BOOLEAN, checkfield3 BOOLEAN,
        log_prevamount numeric, freeamount numeric,
        diff_dates_in_days numeric);
        """
    )


def get_data_from_db():
    data = retrieve_data_from_db(
        """
        SELECT applicationid, amount,
        case WHEN durationindays = '' THEN 0::numeric
        else to_number(durationindays, '9999.9')  end as durationindays,
        checkfield1, checkfield2, checkfield2, prevamount,
        DATE_PART('day', to_date(finishdate, 'YY-MM-DD')::timestamp -
        to_date(startdate, 'YY-MM-DD')::timestamp) as diff_dates_in_days,
        earned - spendings as free_amount FROM public.rawdata
        """
    )
    return data


def calculate_params(data):
    print(data)
    applicationid = data.get('applicationid')
    amount = data.get('amount')
    durationindays = data.get('durationindays')
    checkfield1 = data.get('checkfield1')
    checkfield2 = data.get('checkfield2')
    checkfield3 = data.get('checkfield3')
    prevamount = data.get('prevamount')
    diff_dates_in_days = data.get('diff_dates_in_days')
    free_amount = data.get('free_amount')

    log_amount = np.log(amount)
    log_prevamount = np.log(prevamount)

    if free_amount == 0:
        log_free_amount = 0.0
    elif np.isnan(np.log(free_amount)):
        log_free_amount = 0.0
    else:
        log_free_amount = np.log(free_amount)
    values = [
        applicationid, log_amount, durationindays,
        checkfield1, checkfield2, checkfield3,
        log_prevamount, log_free_amount, diff_dates_in_days
    ]
    return values


def write_data_in_db(data):
    for row in data:
        final_params = calculate_params(row)
        execute_sql(
        """
        Insert INTO public.finaldataset
        (application_id, amount, durationindays,
        checkfield1, checkfield2, checkfield3, log_prevamount,
        freeamount, diff_dates_in_days)
        VALUES (%s)
        """, final_params
        )


def etl_process():
    cur = get_connection()
    create_table()
    raw_data = load_data_from_file(cur)
    data_from_db = get_data_from_db()
    write_data_in_db(data_from_db)
