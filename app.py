from scraping import wid_tools as wt
from etl.stack_records_wide_to_long import crawl_and_process_bucket
import pandas as pd


def handler(event, context):
    '''
    Lambda-ready function to auth to WID, scrape, push to s3. Made redundant by wid_scraper(), will deprecate
    once Lambdas are switched over to use wid_scraper() instead
    :param event: lambda default requirement
    :param context: lambda default requirement
    '''
    w = wt.WIDTrader()
    w.authenticate()
    w.log_market_data()


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
    w.log_market_data()


def raise_error(event, context):
    '''
    Lambda-ready function to assert error and test email failures
    :param event: lambda default requirement
    :param context: lambda default requirement
    '''
    raise ValueError('This is an error')


def process_prices_last_month(event, context):
    """
    Lambda handler that expects a file in s3 to be specified as a target. Target file will be processed from wide
    orientation to long
    :param event: lambda default requirement
    :param context: lambda default requirement
    """
    download_bucket = 'wid-prices'
    upload_bucket = 'wid-prices-processed'
    last_month_bucket_str = (pd.Timestamp.now() - pd.DateOffset(month=1)).strftime('%Y/%m')
    print(f'Processing files in bucket s3://{download_bucket}/{last_month_bucket_str}')
    crawl_and_process_bucket(
        download_bucket=download_bucket,
        upload_bucket=upload_bucket,
        prefix=last_month_bucket_str
        )
