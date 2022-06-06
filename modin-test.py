from logging.config import dictConfig
import boto3
import json
import io
import numpy as np
import oyaml
import pandas as pd
import time
import ray
from dask import dataframe as dd
from modin import pandas as mpd
from multiprocessing import Pool


s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
ray.init()

def grab_keys(bucket: str, filter: str):
    return s3.list_objects_v2(Bucket=bucket, Prefix=filter)["Contents"]

def pd_read_s3_parquet(job) -> dd:
    buffer = io.BytesIO()
    bucket, key, schema = job
    obj = s3_resource.Object(bucket, key)
    obj.download_fileobj(buffer)
    df = mpd.read_parquet(buffer)
    return df, key.replace('/','_')


# Grabs dict with columns and corresponding datatypes
def get_json_from_s3(bucket: str, key: str) -> json:
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, key)
    data = obj.get()["Body"].read().decode("utf-8")
    json_data = oyaml.safe_load(data)

    return json_data


def replace_nulls(df: dd) -> dd:
    return df.replace("'", "").replace(
        [
            "[null]",
            "nan",
            "None",
            "<NA>",
            "null",
            "0",
            "N",
            "F",
            np.inf,
            -np.inf,
            "inf",
            "-inf",
            "np.inf",
            "-np.inf",
            None,
        ],
        np.nan,
    )


def coerce_data(job: list):
    df, key, schema = job
    df = replace_nulls(df)
    for col_name in df.columns:
        dtype = schema.get(col_name, "XXXX")
        if dtype in [
            "Int64",
            "float64",
            "int",
        ]:  # "str" had this
            df[col_name] = dd.to_numeric(df[col_name], errors="coerce")
            if dtype == "Int64":
                df[col_name] = df[col_name].astype(dtype)
        elif dtype in ["datetime64[ns]"]:
            df[col_name] = dd.to_datetime(
                df[col_name], infer_datetime_format=True, errors="coerce"
            )
        else:
            df[col_name] = df[col_name].astype("str")
    # df.to_csv(f"data/{key.replace(['/'], '_').replace('.snappy.parquet','')}.csv", index=False)
    print(df.head())
    return df


def fix_cols(df: dd.DataFrame) -> list:
    cols = [
        c.strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace("-", "_")
        for c in df.columns
    ]
    df.columns = cols
    return df



if __name__ == "__main__":
    bucket = "canvas-data-store-prod"
    filter = "PREPED_C3/10441/2022_05_12/full_canvas/"
    keys = grab_keys(bucket, filter)
    pool = Pool(16)
    schema = get_json_from_s3("canvas-data-types", "pandas_data_type_map.json")
    dfs = pool.map(pd_read_s3_parquet, [(bucket, obj['Key'], schema) for obj in keys])
    s = time.time()
    pool.map(coerce_data, [[df[0], df[1], schema] for df in dfs])
    e = time.time()
    print("download entire s3 bucket = {}".format(e-s))