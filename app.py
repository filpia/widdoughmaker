import wid_tools as wt

def handler(event, context):
    w = wt.WIDTrader()
    w.authenticate()
    w.log_market_data()
