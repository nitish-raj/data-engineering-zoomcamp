# Import libraries
import sys
import pandas as pd
import boto3
from sqlalchemy import create_engine
import argparse
import os

def list_s3_by_prefix(bucket, key_prefix=''):
    # Define S3 client
    s3_client = boto3.client('s3')
    
    next_token = ''
    all_keys = []
    while True:
        if next_token:
            res = s3_client.list_objects_v2(
                Bucket=bucket,
                ContinuationToken=next_token,
                Prefix=key_prefix,
                RequestPayer='requester')
        else:
            res = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=key_prefix,
                RequestPayer='requester')

        try:
            if res['IsTruncated']:
                next_token = res['NextContinuationToken']
            else:
                next_token = ''

            keys = [item['Key'] for item in res['Contents'] if item['Size'] > 0]
            all_keys.extend(keys)
        except:
            break

        if not next_token:
            break

    return all_keys

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    s3_bucket = params.s3_bucket
    s3_key = params.s3_key

    # Download Keys
    source_keys = list_s3_by_prefix(s3_bucket,s3_key)

    # Read data from S3
    data = pd.read_parquet(f's3://{s3_bucket}/{source_keys[0]}')

    data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
    data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)

    # Create engine and write to database

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print("Writing to Postgres")
    data.head(300000).to_sql(name=table_name, con = engine, if_exists='append', index = False, chunksize = 50000, method = 'multi')
    print("Finished writing to Postgres")

if __name__ == '__main__':

    # Define argparse
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # Define arguments
    # user , password , host , port , db , table_name , s3_bucket , s3_key , region
    parser.add_argument('--user', help='Username for Postgres')
    parser.add_argument('--password', help='Password for Postgres')
    parser.add_argument('--host', help='Host for Postgres')
    parser.add_argument('--port', help='Port for Postgres')
    parser.add_argument('--db', help='Database name for Postgres')
    parser.add_argument('--table_name', help='Name of the table where we will write the results to')
    parser.add_argument('--s3_bucket', help='Name of the S3 bucket where the CSV file is located')
    parser.add_argument('--s3_key', help='Name of the S3 key where the CSV file is located')

    # Parse arguments
    args = parser.parse_args()

    main(args)

