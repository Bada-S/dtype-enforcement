import time
from modin import pandas as mpd
import swifter
from distributed import Client
import boto3
import pandas as pd
import oyaml
from dask import dataframe as dd
import ray
import numpy as np
ray.init()

s = time.time()
df = pd.read_parquet("data/data_0_0_0.snappy.parquet", engine="pyarrow")
e = time.time()
print("Pandas Loading Time = {}".format(e-s))

s = time.time()
mdf = mpd.read_parquet("data/data_0_0_0.snappy.parquet", engine="pyarrow")
e = time.time()
print("Modin Pandas Loading Time = {}".format(e-s))



s = time.time()
mdf = mdf.replace("'", "").replace(
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
e = time.time()
print("mdf replace time = {}".format(e-s))

s = time.time()
mdf = mdf.replace("'", "").replace(
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
def get_json_from_s3(bucket: str, key: str):
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, key)
    data = obj.get()["Body"].read().decode("utf-8")
    json_data = oyaml.safe_load(data)

    return json_data
schema = get_json_from_s3(
        "canvas-data-types", "pandas_data_type_map.json"
    )
for col_name in mdf.columns:
    dtype = schema.get(col_name, "XXXX")
    if dtype in [
        "Int64",
        "float64",
        "int",
    ]:  # "str" had this
        mdf[col_name] = pd.to_numeric(mdf[col_name], errors="coerce")
        if dtype == "Int64":
            mdf[col_name] = mdf[col_name].astype(dtype)
        elif dtype in ["datetime64[ns]"]:
            mdf[col_name] = pd.to_datetime(
                mdf[col_name], infer_datetime_format=True, errors="coerce"
            )
        else:
            mdf[col_name] = mdf[col_name].astype("str")
e = time.time()
print("Modin df coerce time = {}".format(e-s))