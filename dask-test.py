import time
import boto3
import pandas as pd
import oyaml
from dask import dataframe as dd
import numpy as np

s = time.time()
df = pd.read_parquet("data_0_0_0.snappy.parquet")
ddf = dd.from_pandas(df, npartitions=8)
e = time.time()
print("dd Pandas Loading Time = {}".format(e-s))

s = time.time()
ddf = ddf.replace("'", "").replace(
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
print("dd replace time = {}".format(e-s))

s = time.time()
df = pd.read_parquet("data_0_0_0.snappy.parquet")
ddf = dd.from_pandas(df, npartitions=2)
ddf = ddf.replace("'", "").replace(
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
print(ddf.info())
e = time.time()
print("Dask df coerce time = {}".format(e-s))