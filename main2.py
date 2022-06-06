from logging.config import dictConfig
import boto3
import json
import io
import numpy as np
import oyaml
import pandas as pd
import time
from dask import dataframe as dd
from multiprocessing import Pool


s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def grab_keys(bucket: str, filter: str):
    return s3.list_objects_v2(Bucket=bucket, Prefix=filter)["Contents"]

def pd_read_s3_parquet(job) -> dd:
    buffer = io.BytesIO()
    bucket, key = job
    s3.download_file(bucket, key, f"data/{key.replace('/','_')}")


# Grabs dict with columns and corresponding datatypes
def get_json_from_s3(bucket: str, key: str) -> json:
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, key)
    data = obj.get()["Body"].read().decode("utf-8")
    json_data = oyaml.safe_load(data)

    return json_data


def replace_nulls(ddf: dd) -> dd:
    return ddf.replace("'", "").replace(
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
    ddf, key, schema = job
    ddf = replace_nulls(ddf)
    for col_name in ddf.columns:
        dtype = schema.get(col_name, "XXXX")
        if dtype in [
            "Int64",
            "float64",
            "int",
        ]:  # "str" had this
            ddf[col_name] = dd.to_numeric(ddf[col_name], errors="coerce")
            if dtype == "Int64":
                ddf[col_name] = ddf[col_name].astype(dtype)
        elif dtype in ["datetime64[ns]"]:
            ddf[col_name] = dd.to_datetime(
                ddf[col_name], infer_datetime_format=True, errors="coerce"
            )
        else:
            ddf[col_name] = ddf[col_name].astype("str")
    # ddf.to_csv(f"data/{key.replace(['/'], '_').replace('.snappy.parquet','')}.csv", index=False)
    print(ddf.head())
    return ddf


def fix_cols(ddf: dd.DataFrame) -> list:
    cols = [
        c.strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace("-", "_")
        for c in ddf.columns
    ]
    ddf.columns = cols
    return ddf



if __name__ == "__main__":
    bucket = "canvas-data-store-prod"
    # filter = "PREPED_C3/10441/2022_05_12/full_canvas/"
    filter = "PREPED_C3/205153663/2022_02_11/full_canvas/"
    keys = grab_keys(bucket, filter)
    pool = Pool(16)
    # schema = get_json_from_s3("canvas-data-types", "pandas_data_type_map.json")
    ddfs = pool.map(pd_read_s3_parquet, [(bucket, obj['Key']) for obj in keys])
    # s = time.time()
    # pool.map(coerce_data, [[ddf[0], ddf[1], schema] for ddf in ddfs])
    # e = time.time()
    # print("download entire s3 bucket = {}".format(e-s))