#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import time
from sqlalchemy import create_engine
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.csv.gz"


    # download_dataset
    os.system(f"wget {url} -O {csv_name}")
    os.system(f"gunzip {csv_name} ")
    csv_name = "output.csv"

    # connect to engine
    engine= create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # create iteration
    df_itr = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # next chunk
    df = next(df_itr)

    # preporocess date column 
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # add only columns to database
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    # ingest first chunk of data
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time.time()
            
            df = next(df_itr)
            
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time.time()
            
            print(f'appneded some chunk of data to postgres databse with time {t_start - t_end}')
        except StopIteration:
            print('finished ingesting data into database')
            break
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data into postgres')

    parser.add_argument('--user', required=True, help='user to connect to posgres database')
    parser.add_argument('--password', required=True, help='password to connect to posgres database')
    parser.add_argument('--host', required=True, help='host to connect to posgres database')
    parser.add_argument('--port', required=True, help='port to connect to posgres database')
    parser.add_argument('--db', required=True, help='db to connect to in postgres')
    parser.add_argument('--table_name', required=True, help='table name to read and write data posgres database')
    parser.add_argument('--url', required=True, help='url of dataset')

    args = parser.parse_args()
    main(args)

