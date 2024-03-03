'''
Adapted from code at https://github.com/amri/BVTrader
'''
import datetime
from xml.etree import ElementTree
import requests
import json
import pandas as pd
import boto3
import ast
import os

CONFIG_PATH = os.getenv('CONFIG_PATH')
with open(CONFIG_PATH, 'r') as f:
    CONFIG = json.load(f)
USER = CONFIG['username']
PW = CONFIG['password']

class WIDTrader(object):

    API_POST_AUTHENTICATE = "https://www.whiskyinvestdirect.com/secure/j_security_check"
    API_GET_SESSION = "https://www.whiskyinvestdirect.com/secure/login.do"
    API_GET_VIEW_MARKET = "https://www.whiskyinvestdirect.com/secure/api/v2/view_market_xml.do"
    API_STATIC_MARKET = "http://www.whiskyinvestdirect.com/view_market_xml.do"
    API_GET_BALANCE = "https://www.whiskyinvestdirect.com/secure/api/v2/view_balance_xml.do"
    API_GET_CHART_TEMPLATE = "https://www.whiskyinvestdirect.com/{distillery}/{bondYear}/{bondQuarter}/{barrelTypeCode}/chart.do"
    S3_BUCKET = "wid-prices-test"

    session = requests.Session()
    raw_market_data = None
    s3_session = boto3.Session()
    s3 = s3_session.client('s3')

    def authenticate(self):
        creds = {'j_username': USER, 'j_password': PW}
        login_resp = self.session.get(self.API_GET_SESSION)
        login_auth_resp = self.session.post(self.API_POST_AUTHENTICATE, data = creds, headers = login_resp.cookies)

        return login_auth_resp

    def get_raw_market_data(self):
        request = self.session.get(self.API_GET_VIEW_MARKET)
        #xml_file = request.data
        return request.content

    def get_static_market_data(self):
        request = self.session.get(self.API_STATIC_MARKET)

        return request.content

    def get_balance(self):
        request = self.session.get(self.API_GET_BALANCE)

        return request.content


    def get_chart(self,attribs):
        request = self.session.get(self.API_GET_CHART_TEMPLATE.format(**attribs))

        return request.content


    def put_to_s3(self, bucket, key, fpath):
        f_obj = self.s3.put_object(Bucket=bucket, Key=key, Body=fpath)
        return


    def log_market_data(self):
        now = datetime.datetime.now()
        md = self.get_market_data()
        self.put_to_s3(self.S3_BUCKET, now.strftime('%Y/%m/%d/prices_%H%M%S.csv'), md.to_csv())
        return


    def get_market_data(self):
        if self.raw_market_data == None:
            self.raw_market_data = ElementTree.fromstring(self.get_raw_market_data())
        records = []
        for pitch in self.raw_market_data.iter("pitch"):
            rec = {'security_id': pitch.get("securityId"),
                    'distillery': pitch.get('distillery'),
                    'category': pitch.get('categoryName'),
                    'barrel_type': pitch.get('barrelTypeCode'),
                    'year': pitch.get('bondYear'),
                    'qtr': pitch.get('bondQuarter'),
                    'currency': pitch.get("considerationCurrency")}
            purchase_options = list(list(pitch)[0])
            sell_options = list(list(pitch)[1])
            i = 0
            for b in purchase_options:
                rec[f'po_{i}_qty'] = b.get('quantity')
                rec[f'po_{i}_price'] = b.get('limit')
                i+=1
            i = 0
            for s in sell_options:
                rec[f'so_{i}_qty'] = s.get('quantity')
                rec[f'so_{i}_price'] = s.get('limit')
                i+=1
            records.append(rec)
        return pd.DataFrame(records)

class WIDDoughMaker():

    def setUp(self):
        self.trader = WIDTrader()
        self.logged_session = None
        self.pitch_path = "pitches.xml"
        self.all_pitches_in_mkt = None

    def fetch_pitches(self):
        content = self.trader.get_static_market_data()

        with open(self.pitch_path, "w") as f:
            f.write(content)
        return

    def get_pitches(self):
        self.fetch_pitches()

        tree = ElementTree.parse(self.pitch_path)
        root = tree.getroot()

        pitches = []

        for pitch in root.iter('pitch'):
            pitches.append(pitch.attrib)

        r = pd.DataFrame(pitches)
        r['distillery'] = r['distillery'].str.lower()

        self.all_pitches_in_mkt = r.drop_duplicates(['barrelTypeCode','bondQuarter',
                                                     'bondYear','categoryName',
                                                     'distillery'])
        print("Num of pitches:{}".format(self.all_pitches_in_mkt.shape[0]))

        return

    def get_chart_data(self, attribs = None):
        #return self.all_pitches_in_mkt.iloc[0]

        if attribs == None:
            attribs = self.all_pitches_in_mkt.iloc[0].to_dict()

        r = self.trader.get_chart(attribs)

        # want to clip xml string

        chart_data = r.split("Chart.drawChart( $('#chartContainer'), ")[1].split(", 'USD'")[0]
        chart_df = pd.DataFrame(ast.literal_eval(chart_data))
        chart_df['dealDate'] = chart_df['dealDate'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000).date() )
        chart_df['dummy'] = 1

        attribs_df = pd.DataFrame([attribs])
        attribs_df['dummy'] = 1

        return chart_df.merge(attribs_df,ons='dummy')

    def pitches(self):
        return self.all_pitches_in_mkt()

    def market_rate(self, secID='SPIRIT000218', curr='USD'):
        try:
            r = self.trader.get_market_data()
            if r is None:
                return {
                    'security_id': secID,
                    'notes': 'failure'
                    }
            else:
                return r
        except Exception:
            return {
                'security_id':secID,
                'notes':'failure'
                }

    def login(self):
        response = self.trader.authenticate()

        self.logged_session = response

        return response

    def alerts_helper(self,secID,base_price,alert_thresh_pct):
        mr = self.market_rate(secID,'USD')

        if mr['current_buy_price']>base_price*(1+alert_thresh_pct):
            return mr
        else:
            return {}


    def alerts(self,alert_config):
        alert_df = pd.read_csv(alert_config)
        alert_items = ['barrel_type','distillery','qtr','year','security_id','current_buy_price']

        alerts = []

        for f in alert_df.iterrows():
            whisky = f[1]
            alert_output = self.alerts_helper(whisky['security_id'],whisky['base_price'],whisky['notification_floor_pct'])

            if alert_output != {}:
                new_alert = {}
                new_alert['security_id'] = whisky['security_id']
                new_alert['base_price'] = whisky['base_price']

                for a in alert_items:
                    new_alert[a] = alert_output[a]

                alerts.append(new_alert)

        return alerts
