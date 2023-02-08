#!/usr/bin/env python
# coding: utf-8

import argparse
import os

from sqlalchemy import create_engine
import pandas as pd
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    #download csv

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')



    while True:

        try:
            t_start = time()
            
            df = next(df_iter)
            
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_stop = time()
            
            print('inserted another chunk..., took %.3f second' %(t_stop - t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Injest CSV to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='hostname for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table results will be stored in')
    parser.add_argument('--url', help='url of the csv')

    args = parser.parse_args()

    main(args)

