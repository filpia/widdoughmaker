'''
Adapted from code at https://github.com/amri/BVTrader
'''
import datetime
import time
from xml.etree import ElementTree
import requests
import unittest
import urllib3
import certifi
import configparser
import json
from xml.etree import ElementTree as ET
import pandas as pd
import matplotlib.pyplot as plt
import ast

class WIDTrader(object):

    API_POST_AUTHENTICATE = "https://www.whiskyinvestdirect.com/secure/j_security_check"
    API_GET_SESSION = "https://www.whiskyinvestdirect.com/secure/login.do"
    API_GET_VIEW_MARKET = "https://www.whiskyinvestdirect.com/secure/api/v2/view_market_xml.do"
    API_STATIC_MARKET = "http://www.whiskyinvestdirect.com/view_market_xml.do"
    API_GET_BALANCE = "https://www.whiskyinvestdirect.com/secure/api/v2/view_balance_xml.do"
    API_GET_CHART_TEMPLATE = "https://www.whiskyinvestdirect.com/{distillery}/{bondYear}/{bondQuarter}/{barrelTypeCode}/chart.do"

    def authenticate(self):
        config = json.load(open("config/configuration.txt"))
        user = config['username']
        pasw = config['password']
        response = requests.get(self.API_GET_SESSION)
        response = requests.post(self.API_POST_AUTHENTICATE, cookies=response.cookies, data={'j_username': user, 'j_password': pasw})
        return response

    def get_raw_market_data(self):
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
        request = http.request("GET", self.API_GET_VIEW_MARKET)
        xml_file = request.data
        return xml_file
    
    def get_static_market_data(self):
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
        request = http.request("GET", self.API_STATIC_MARKET)
        xml_file = request.data
        return xml_file
    
    def get_balance(self):
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
        request = http.request("GET", self.API_GET_BALANCE)
        xml_file = request.data
        return xml_file
    
    def get_chart(self,attribs):
        # {distillery}/{bondYear}/{bondQuarter}/{barrelTypeCode}
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())

        request = http.request("GET", self.API_GET_CHART_TEMPLATE.format(**attribs))
        xml_file = request.data
        return xml_file
        

    def get_market_data(self, security_class, security_id, currency):
        root = ElementTree.fromstring(self.get_raw_market_data())

        i = 0
        for market in root.iter("pitch"):
            sec_class = market.get("securityClassNarrative")
            sec_id = market.get("securityId")
            curr = market.get("considerationCurrency")
            if sec_class == security_class and sec_id == security_id and curr == currency:
                buy_price = list(list(market)[0])[0]
                sell_price = list(list(market)[1])[0]
                print("Buy: {0} at {1}".format(buy_price.get("quantity"), buy_price.get("limit")))
                print("Sell: {0} at {1}".format(sell_price.get("quantity"), sell_price.get("limit")))


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
        
        tree = ET.parse(self.pitch_path)
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
        
        return chart_df.merge(attribs_df,on='dummy')
    
    def pitches(self):
        return self.all_pitches_in_mkt
    
        
    def login(self):
        response = self.trader.authenticate()

        self.logged_session = response
        
        return
