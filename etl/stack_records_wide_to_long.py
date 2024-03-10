"""
Processing script to take wide files and transform to long

Traverse s3 bucket to find raw files which don't have processed counterpart
"""
import boto3
import io
import pandas as pd
from io import BytesIO, StringIO
import os
from pathlib import Path
import re

RAW_BUCKET = 'wid-prices'
PROCESSED_BUCKET = 'wid-prices-processed'


def wide_to_long(df, key):
    """
    Given a wide dataframe for a single whisky and columns for each buy/sell order, transform the columns into rows
    where the resulting dataframe is long with one row per buy/sell order
    :param df: wide dataframe
    :param key: s3 file key
    :return: long dataframe
    """
    year, month, day, fname = key.split('/')
    hours = fname.split('.')[0][-6:-4]
    minutes = fname.split('.')[0][-4:-2]
    seconds = fname.split('.')[0][-2:]

    dt_str = f'{year}/{month}/{day} {hours}:{minutes}:{seconds}'

    gcols = ['barrel_type', 'category', 'currency', 'distillery', 'year', 'security_id', 'qtr']
    tmp = df.copy()
    if 'Unnamed: 0' in tmp.columns:
        tmp.drop(['Unnamed: 0'], axis=1, inplace=True)
    tmp = tmp.set_index(gcols).stack().reset_index()
    tmp.columns = gcols + ['value_field', 'value']
    # TODO: how to filter and alert if weird values creep in..
    # IDEA: filter out null column name first then assert all cols start with po or so
    # Current values seem to begin with po_ and so_
    tmp = tmp[tmp['value_field'].str.startswith('po_') |
              tmp['value_field'].str.startswith('so_')
              ].copy()
    tmp['happened_at'] = dt_str

    tmp['action_type'] = tmp['value_field'].apply(lambda x: x.split('_')[0])
    tmp['value_type'] = tmp['value_field'].apply(lambda x: x.split('_')[-1])
    tmp['value_type_order'] = tmp['value_field'].apply(lambda x: x.split('_')[1])
    tmp.drop(['value_field'], axis=1, inplace=True)
    return tmp


def upload_df_to_s3(df, upload_bucket, upload_key, s3_client):
    """
    Read file from disk, write data to s3 bucket
    :param df: dataframe of wide whiskey prices, one col for each buy/sell offer and qty
    :param upload_bucket: bucket to upload file to
    :param upload_key: file name to give to uploaded file
    :return: None
    """
    # TODO: modify this to conditionally write parquet or csv
    buffer_to_upload = io.StringIO()
    df.to_csv(buffer_to_upload, index=False)
    buffer_to_upload.seek(0)
    s3_client.put_object(Body=buffer_to_upload.getvalue(), Bucket=upload_bucket, Key=upload_key)
    return None


def read_wide_df_from_s3(download_bucket, key, s3_client):
    """
    Download bytes object from s3 into in-memory buffer. Read into dataframe and return
    :param download_bucket: s3 bucket to download from
    :param key: path in bucket where file is downloaded from/written to
    :return: Dataframe of whiskey prices
    """
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


def prices_wide_to_long(download_bucket, key, upload_bucket, s3_client):
    """
    Pull a wide file from download_bucket with path key, transform it to long file and write to another bucket
    using name key as path
    :param download_bucket: s3 bucket to download from
    :param upload_bucket: s3 bucket to upload to
    :param key: path in bucket where file is downloaded from/written to
    :param staging_fname: name of file on local disk
    :return:
    """

    wide_df = read_wide_df_from_s3(download_bucket=download_bucket, key=key, s3_client=s3_client)
    long_df = wide_to_long(wide_df, key)
    upload_df_to_s3(df=long_df, upload_bucket=upload_bucket, upload_key=key, s3_client=s3_client)
    return long_df


def crawl_and_process_bucket(download_bucket, upload_bucket, prefix='', max_items_to_process=1000):
    """_summary_
    Initialize an s3 client and list objects in download_bucket matching a prefix. Check whether to process
    that file by seeing whether there's an accompanying one in the upload_bucket.

    If Exception is hit, create a local cookie file to denote the problematic record for future resolution
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=download_bucket,
        Prefix=prefix,
        PaginationConfig={
            'MaxItems': max_items_to_process
        }
        )
    for page in page_iterator:
        for obj in page['Contents']:
            processed_files_pref = s3_client.list_objects(Bucket=upload_bucket, Prefix=obj['Key'])
            if len(processed_files_pref.get('Contents', [])) == 0:
                try:
                    prices_wide_to_long(
                        download_bucket=download_bucket,
                        key=obj['Key'],
                        upload_bucket=upload_bucket,
                        s3_client=s3_client
                        )
                except Exception as e:
                    print(f"Error processing {obj['Key']}. Error: {str(e)}")
                    Path(f"errors_processing/{obj['Key']}").mkdir(parents=True, exist_ok=True)
                    os.system(f"touch errors_processing/{obj['Key']}.txt")
            else:
                print(f"Skipping {obj['Key']} because processed version already exists")


if __name__ == "__main__":
    # modify prefix as needed YEAR/MONTH/DAY/FILE_NAME
    # days = list(map(lambda x: x.strftime("%Y/%m/%d"), pd.date_range('2022-03-08','2024-03-01', freq='D')))
    days = pd.date_range('2022-03-08','2024-03-01', freq='D')
    for day_as_bucket in days:
        print(day_as_bucket)
        crawl_and_process_bucket(
            download_bucket=RAW_BUCKET,
            upload_bucket=PROCESSED_BUCKET,
            prefix=day_as_bucket.strftime("%Y/%m/%d"),
            max_items_to_process=20
        )