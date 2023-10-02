import wid_tools as wt


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
