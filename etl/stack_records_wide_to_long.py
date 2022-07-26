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
processed_bucket = 'wid-prices-processed'
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
    Using a local staging file, write data to s3 bucket
    :param staging_fname: local file to be used as staging file
    :param upload_bucket: bucket to upload file to
    :param upload_key: file name to give to uploaded file
    :return: None
    '''
    buffer_to_upload = io.BytesIO()
    with open(staging_fname, 'r') as inFile:
        r = csv.reader(inFile)
        # copy the rest
        for i, row in enumerate(r):
            if i == 0:
                row[0] = 'index'
            buffer_to_upload.write((','.join(row) + '\n').encode('utf-8'))
    buffer_to_upload.seek(0)
    # easier to manipulate as df
    df = pd.read_csv(buffer_to_upload)
    df = stack_df(df, upload_key)
    buffer_to_upload = io.StringIO()
    df.to_csv(buffer_to_upload, index=False)
    buffer_to_upload.seek(0)
    s3_client.put_object(Body=buffer_to_upload.getvalue(), Bucket=upload_bucket, Key=upload_key)
    return None


staging_fname = 'stage.csv'
paginator = s3_client.get_paginator('list_objects')
page_iterator = paginator.paginate(Bucket=raw_bucket)

for page in page_iterator:
    for obj in page['Contents']:
        processed_files_pref = s3_client.list_objects(Bucket=processed_bucket, Prefix=obj['Key'])
        if len(processed_files_pref.get('Contents', [])) == 0:
            s3_client.download_file(Bucket=raw_bucket, Key=obj['Key'], Filename=staging_fname)
            try:
                fix_and_upload(staging_fname, processed_bucket, obj['Key'])
            except Exception as e:
                print(f"Error processing {obj['Key']}")
                Path(f"errors_processing/{obj['Key']}").mkdir(parents=True, exist_ok=True)
                os.system(f"touch /home/ec2-user/SageMaker/wid-prices/errors_processing/{obj['Key']}.txt")
                pass
        else:
            print(f"Skipping {obj['Key']} because processed version already exists")
