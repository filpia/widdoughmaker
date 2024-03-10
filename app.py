from scraping import wid_tools as wt
from etl.stack_records_wide_to_long import crawl_and_process_bucket, prices_wide_to_long
import pandas as pd
import boto3

DOWNLOAD_BUCKET = 'wid-prices'
UPLOAD_BUCKET = 'wid-prices-processed'


def handler(event, context):
    '''
    Lambda-ready function to auth to WID, scrape, push to s3. Made redundant by wid_scraper(), will deprecate
    once Lambdas are switched over to use wid_scraper() instead
    :param event: lambda default requirement
    :param context: lambda default requirement
    '''
    w = wt.WIDTrader()
    w.authenticate()
    w.log_market_data(format='parquet')


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
    w.log_market_data(format='parquet')


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
    prices_wide_to_long(
        download_bucket=s3_info['bucket']['name'],
        key=s3_info['object']['key'],
        upload_bucket=UPLOAD_BUCKET,
        s3_client=boto3.client('s3')
    )


def process_prices_last_month(event, context):
    """
    Lambda handler that expects a file in s3 to be specified as a target. Target file will be processed from wide
    orientation to long
    :param event: lambda default requirement
    :param context: lambda default requirement
    """
    
    
    last_month_bucket_str = (pd.Timestamp.now() - pd.DateOffset(month=1)).strftime('%Y/%m')
    print(f'Processing files in bucket s3://{DOWNLOAD_BUCKET}/{last_month_bucket_str}')
    crawl_and_process_bucket(
        download_bucket=DOWNLOAD_BUCKET,
        upload_bucket=UPLOAD_BUCKET,
        prefix=last_month_bucket_str
        )
