'''
Adapted from code at https://github.com/amri/BVTrader
'''
import datetime
import time
from xml.etree import ElementTree
import requests
import urllib3
import configparser
import json
import pandas as pd
import ast

class WIDTrader(object):

    API_POST_AUTHENTICATE = "https://www.whiskyinvestdirect.com/secure/j_security_check"
    API_GET_SESSION = "https://www.whiskyinvestdirect.com/secure/login.do"
    API_GET_VIEW_MARKET = "https://www.whiskyinvestdirect.com/secure/api/v2/view_market_xml.do"
    API_STATIC_MARKET = "http://www.whiskyinvestdirect.com/view_market_xml.do"
    API_GET_BALANCE = "https://www.whiskyinvestdirect.com/secure/api/v2/view_balance_xml.do"
    API_GET_CHART_TEMPLATE = "https://www.whiskyinvestdirect.com/{distillery}/{bondYear}/{bondQuarter}/{barrelTypeCode}/chart.do"
    
    session = requests.Session()
    raw_market_data = None

    def authenticate(self):
        config = json.load(open("config/configuration.txt"))
        user = config['username']
        pasw = config['password']
        creds = {'j_username': user, 'j_password': pasw}
        
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
        

    def get_market_data(self, security_id, currency):
        if self.raw_market_data == None:
            self.raw_market_data = ElementTree.fromstring(self.get_raw_market_data())
            
        for market in self.raw_market_data.iter("pitch"):
            sec_id = market.get("securityId")
            curr = market.get("considerationCurrency")
            if sec_id == security_id and curr == currency:
                dist = market.get('distillery')
                cat = market.get('categoryName')
                barrel = market.get('barrelTypeCode')
                yr = market.get('bondYear')
                qtr = market.get('bondQuarter')
                
                buy_price = list(list(market)[0])[0]
                sell_price = list(list(market)[1])[0]
                
                return {'security_id':security_id,
                        'distillery':dist,
                        'category':cat,
                        'barrel_type':barrel,
                        'year':yr,
                        'qtr':qtr,
                        'current_buy_price':float(buy_price.get("limit")),
                        'current_sell_price':float(sell_price.get("limit")),
                        'currency':currency,
                        'notes':'success'}

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
    
    def market_rate(self,secID='SPIRIT000218',curr='USD'):
        try:
            r = self.trader.get_market_data(secID,curr)
            if r == None:
                return {'security_id':secID,
                        'notes':'failure'}
            else:
                return r
        except:
            return {'security_id':secID,
                        'notes':'failure'}
        
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
            