'''
Initially, the pipeline wrote csv files. After a refactor the pipeline now writes parquet files.

This script is intended to copy over contents from a s3 bucket containing csv files into another bucket
containing .parquet files. Each csv will be read into a pandas dataframe before being written as parquet.
'''

import pandas as pd
import boto3
import os
from pathlib import Path

from utils import read_s3_to_dataframe, upload_df_to_s3

def copy_csv_bucket_to_parquet(csv_bucket, parquet_bucket, prefix):
    """
    Use an iterator to get all objects in csv bucket

    For each file...
        Check whether a file exists in the parquet bucket with key.replace('csv', 'parquet')
        - if no, write dataframe to parquet bucket

    """

    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=csv_bucket,
        Prefix=prefix
        )
    for page in page_iterator:
        for obj in page['Contents']:
            parquet_key = obj['Key'].replace('.csv', '.parquet')
            processed_files_pref = s3_client.list_objects(Bucket=parquet_bucket, Prefix=parquet_key)
            if len(processed_files_pref.get('Contents', [])) == 0:
                try:
                    # print(f"Copying {obj['Key']} to {parquet_key}")
                    df = read_s3_to_dataframe(
                        download_bucket=csv_bucket,
                        key=obj['Key'],
                        s3_client=s3_client
                        )
                    upload_df_to_s3(
                        df=df,
                        upload_bucket=parquet_bucket,
                        upload_key=parquet_key
                        )
                except Exception as e:
                    print(f"Error processing {obj['Key']}. Error: {str(e)}")
                    Path(f"errors_processing/{obj['Key']}").mkdir(parents=True, exist_ok=True)
                    os.system(f"touch errors_processing/csv_to_parquet_{obj['Key']}")
            else:
                print(f"Skipping {obj['Key']} because processed version already exists {parquet_key}")
    return


if __name__ == "__main__":
    days = pd.date_range('2019-04-07','2024-03-15', freq='D')
    for day_as_bucket in days:
        print(day_as_bucket)
        copy_csv_bucket_to_parquet(
            csv_bucket='wid-prices',
            parquet_bucket='wid-prices-parquet',
            prefix=day_as_bucket.strftime("%Y/%m/%d")
        )