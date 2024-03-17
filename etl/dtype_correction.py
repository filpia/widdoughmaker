'''
Determined that dtypes of dataframes being written to parquet changed around 12/15/2023

Running this script to pull files from s3 and cast dtypes to match types from previous runs.

Goes along with a change to stack_records_wide_to_long.py to assert types on all cols
'''

import pandas as pd
import boto3
import os
from pathlib import Path
from etl.utils import read_s3_to_dataframe, upload_df_to_s3
import numpy as np


def cast_dtypes(df):
    dtype_mapping = {
        'value': np.float64,
        'year': np.int64
    }
    for col in df.columns:
        df[col] = df[col].astype(dtype_mapping.get(col, str))
    return df

def loop_correct_dtypes(parquet_bucket, prefix):
    """
    Use an iterator to get all objects in csv bucket

    For each file...
        Check whether a file exists in the parquet bucket with key.replace('csv', 'parquet')
        - if no, write dataframe to parquet bucket

    """

    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=parquet_bucket,
        Prefix=prefix
        )
    for page in page_iterator:
        for obj in page['Contents']:
            try:
                df = read_s3_to_dataframe(
                            download_bucket=parquet_bucket,
                            key=obj['Key'],
                            s3_client=s3_client
                            )
                df = cast_dtypes(df)
                upload_df_to_s3(
                        df=df,
                        upload_bucket=parquet_bucket,
                        upload_key=obj['Key']
                        )    
            except Exception as e:
                print(f"Error processing {obj['Key']}. Error: {str(e)}")
                Path(f"errors_processing/{obj['Key']}").mkdir(parents=True, exist_ok=True)
                os.system(f"touch errors_processing/dtype_correction_{obj['Key']}")
    return


if __name__ == "__main__":
    from joblib import Parallel, delayed

    days = pd.date_range('2020-03-01','2024-03-15', freq='D')
    for day_as_bucket in days:
        loop_correct_dtypes(
            parquet_bucket='wid-prices-parquet',
            prefix=day_as_bucket.strftime("%Y/%m/%d")
        )
    # Parallel(
    #     n_jobs=-2,
    #     verbose=True,
    #     prefer='threads')(delayed(loop_correct_dtypes)(
    #         csv_bucket='wid-prices',
    #         parquet_bucket='wid-prices-parquet',
    #         prefix=day_as_bucket.strftime("%Y/%m/%d")
    #     ) for day_as_bucket in days)