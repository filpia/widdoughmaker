'''
Reusable helpers across various ETL scripts
'''
from io import BytesIO, StringIO
import re
import pandas as pd
import boto3


def read_s3_to_dataframe(download_bucket, key, s3_client=None):
    """
    Download bytes object from s3 into in-memory buffer.
    Read into dataframe and return. Use file extension to infer type
    and use appropriate method to read in
    :param download_bucket: s3 bucket to download from
    :param key: path in bucket where file is downloaded from/written to
    :return: Dataframe
    """
    if s3_client is None:
        s3_client = boto3.client('s3')
    bio = BytesIO()
    s3_client.download_fileobj(Bucket=download_bucket, Key=key, Fileobj=bio)
    bio.seek(0)
    if len(re.findall('.csv$', key))>0:
        print(f'CSV file detected. Reading csv file {key}')
        return pd.read_csv(StringIO(bio.read().decode('utf-8')))
    if len(re.findall('.parquet$', key))>0:
        print(f'Parquet file detected. Reading parquet file {key}')
        return pd.read_parquet(bio)
    raise ValueError(f'Key must end with either .csv or .parquet. {key}')

def upload_df_to_s3(df, upload_bucket, upload_key):
    """
    Read file from disk, write data to s3 bucket. Infer type of file to write based
    on file extension in upload_key
    :param df: dataframe of wide whiskey prices, one col for each buy/sell offer and qty
    :param upload_bucket: bucket to upload file to
    :param upload_key: file name to give to uploaded file. If ends with .csv, file written
        as csv. If ends with .parquet, file written as parquet. Else ValueError raised
    """
    assert isinstance(df, pd.DataFrame)
    if len(re.findall('.csv$', upload_key))>0:
        df.to_csv(f's3://{upload_bucket}/{upload_key}', index=False)
        return
    elif len(re.findall('.parquet$', upload_key))>0:
        df.to_parquet(f's3://{upload_bucket}/{upload_key}', index=False)
        return
    else:
        raise ValueError('format must be one of csv or parquet')