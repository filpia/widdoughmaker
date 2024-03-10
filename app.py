from scraping import wid_tools as wt
from etl.stack_records_wide_to_long import crawl_and_process_bucket, prices_wide_to_long
import pandas as pd
import boto3
import re

DOWNLOAD_BUCKET_DICT = {
    'csv': 'wid-prices',
    'parquet': 'wid-prices-parquet'
}
UPLOAD_BUCKET_DICT = {
    'csv': 'wid-prices-processed',
    'parquet': 'wid-prices-processed-parquet'
}


def handler(event, context):
    '''
    Lambda-ready function to auth to WID, scrape, push to s3. Made redundant by wid_scraper(), will deprecate
    once Lambdas are switched over to use wid_scraper() instead
    :param event: lambda default requirement
    :param context: lambda default requirement
    '''
    w = wt.WIDTrader()
    w.authenticate()
    w.log_market_data(s3_bucket_dict=DOWNLOAD_BUCKET_DICT, output_format='both')


def log_wid_prices(event, context):
    '''
    Lambda-ready function to auth to WID, scrape, push to s3. Made redundant by wid_scraper(), will deprecate
    once Lambdas are switched over to use wid_scraper() instead
    :param event: lambda default requirement
    :param context: lambda default requirement
    '''
    print('In log_wid_prices()')
    w = wt.WIDTrader()
    w.authenticate()
    w.log_market_data(s3_bucket_dict=DOWNLOAD_BUCKET_DICT, output_format='both')


def raise_error(event, context):
    '''
    Lambda-ready function to assert error and test email failures
    :param event: lambda default requirement
    :param context: lambda default requirement
    '''
    raise ValueError('This is an error')


def process_file_s3_trigger(event, context):
    """_summary_
    Triggered when a file is added to s3 bucket this lambda is configured for. Processes that file from wide to long

    :param event: information about the event that triggered this lambda, specifically the s3 file location
    :type event: dict
    :param context: Instance of LambdaContext, mostly Lambda metadata 
    :type context: LambdaContext
    """
    s3_info = event['Records'][0]['s3']
    if len(re.findall('.csv$', s3_info['object']['key'])):
        upload_bucket = UPLOAD_BUCKET_DICT['csv']
    elif len(re.findall('.parquet$', s3_info['object']['key'])):
        upload_bucket = UPLOAD_BUCKET_DICT['parquet']
    else:
        raise ValueError(f"File {s3_info['object']['key']} must have extension .csv or .parquet")
    prices_wide_to_long(
        download_bucket=s3_info['bucket']['name'],
        key=s3_info['object']['key'],
        upload_bucket=upload_bucket,
        s3_client=boto3.client('s3')
    )
