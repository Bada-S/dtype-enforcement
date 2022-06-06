import pandas as pd
import os
import glob
import swifter as sft

# path = "data"
# csv_files = os.listdir(path)
# for f in csv_files:
#     df = pd.read_parquet("data/" +f)
#     df.to_csv(f"csvs/{f.split('.')[0]}.csv", index=False)


def read_dfs():
    path = "csvs"
    csv_files = os.listdir(path)
    dfs = [pd.read_csv(f"csvs/{f.split('.')[0]}.csv") for f in csv_files]
    return dfs

def coerce_df(df):
    