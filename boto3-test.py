import boto3
import io
import pandas as pd
from dask import dataframe as dd
import oyaml
import json
import numpy as np
import time
from multiprocessing import Pool

s3 = boto3.client("s3")

def grab_keys(bucket: str, filter: str):
    return s3.list_objects_v2(Bucket=bucket, Prefix=filter)["Contents"]


def initialize():
  global s3_client
  s3_client = boto3.client('s3')


# Read single parquet file from S3
# def pd_read_s3_parquet(job):
    # bucket, key, name = job
    # obj = s3.get_object(Bucket=bucket, Key=key)
#     return pd.read_parquet(io.BytesIO(obj['Body'].read()))

def pd_read_s3_parquet(job) -> dd:
    buffer = io.BytesIO()
    bucket, key = job
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    obj.download_fileobj(buffer)
    df = pd.read_parquet(buffer)
    return dd.from_pandas(df, npartitions=8)

if __name__ == "__main__":
    s = time.time()
    bucket = "canvas-data-store-prod"
    filter = "PREPED_C3/10441/2022_05_12/full_canvas/"
    keys = grab_keys(bucket, filter)
    print(len([obj['Key'] for obj in keys]))
    pd_read_s3_parquet([bucket, 'PREPED_C3/10441/2022_05_12/full_canvas/data_2_3_0.snappy.parquet'])
    # pool = Pool(8, initialize)
    # pool.map(pd_read_s3_parquet, [(bucket, obj['Key']) for obj in keys])
    e = time.time()
    print("download entire s3 bucket = {}".format(e-s))
