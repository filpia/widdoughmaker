'''
Processing script to take a dataframe where ordinal buy/sell orders are added as columns and transform to case where
they are represented as rows. Check s3 to see if a processed version of the timestamped file exists before processing it

Pseudocode:
- get all files from raw bucket
- for each file:
    - check if one exists with same key in processed bucket
    - if not: process and write
    - if so: skip

'''
import boto3
import io
import os
import csv
from pathlib import Path
import pandas as pd
import pandas as pd
from pathlib import Path

raw_bucket = 'wid-prices'
bucket = 'wid-prices-processed'
s3_client = boto3.client('s3')


def stack_df(df, key):
    '''
    Given a wide dataframe for a single whisky and columns for each buy/sell order, transform the columns into rows
    where the resulting dataframe is long with one row per buy/sell order
    :param df: wide dataframe
    :param key: s3 file key
    :return: long dataframe
    '''
    year, month, day, fname = key.split('/')
    hours = fname.split('.')[0][-6:-4]
    minutes = fname.split('.')[0][-4:-2]
    seconds = fname.split('.')[0][-2:]

    dt_str = f'{year}/{month}/{day} {hours}:{minutes}:{seconds}'

    gcols = ['barrel_type', 'category', 'currency', 'distillery', 'year', 'security_id', 'qtr']
    tmp = df.set_index(gcols).stack().reset_index()
    tmp.columns = gcols + ['value_field', 'value']
    tmp = tmp.query('value_field!="index"').copy()
    tmp['happened_at'] = dt_str

    tmp['action_type'] = tmp['value_field'].apply(lambda x: x.split('_')[0])
    tmp['value_type'] = tmp['value_field'].apply(lambda x: x.split('_')[-1])
    tmp['value_type_order'] = tmp['value_field'].apply(lambda x: x.split('_')[1])
    tmp.drop(['value_field'], axis=1, inplace=True)
    return tmp


def fix_and_upload(staging_fname, upload_bucket, upload_key):
    '''
    Read file from disk, write data to s3 bucket
    :param staging_fname: local file to be used as staging file
    :param upload_bucket: bucket to upload file to
    :param upload_key: file name to give to uploaded file
    :return: None
    '''
    buffer_to_read = io.BytesIO()
    with open(staging_fname, 'r') as inFile:
        r = csv.reader(inFile)
        # copy the rest
        for i, row in enumerate(r):
            if i == 0:
                row[0] = 'index'
            buffer_to_read.write((','.join(row) + '\n').encode('utf-8'))
    buffer_to_read.seek(0)
    # easier to manipulate as df
    df = pd.read_csv(buffer_to_read)
    df = stack_df(df, upload_key)
    buffer_to_upload = io.StringIO()
    df.to_csv(buffer_to_upload, index=False)
    buffer_to_upload.seek(0)
    s3_client.put_object(Body=buffer_to_upload.getvalue(), Bucket=upload_bucket, Key=upload_key)
    return None


def prices_wide_to_long(download_bucket, key, upload_bucket, staging_fname='stage.csv'):
    """
    Pull a wide file from download_bucket with path key, transform it to long file and write to another bucket
    using name key as path
    :param download_bucket: s3 bucket to download from
    :param upload_bucket: s3 bucket to upload to
    :param key: path in bucket where file is downloaded from/written to
    :param staging_fname: name of file on local disk
    :return:
    """
    s3_client.download_file(Bucket=download_bucket, Key=key, Filename=staging_fname)
    fix_and_upload(staging_fname=staging_fname, upload_bucket=upload_bucket, upload_key=key)
    return



