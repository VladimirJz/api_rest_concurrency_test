from datetime import datetime
from datetime import date
#from sqlalchemy.engine import RowMapping
from sqlalchemy.engine.row import RowMapping

import json

import requests
from decimal import *



class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):

        if isinstance(obj, Decimal):
            return str(obj)

        if isinstance(obj, datetime):
            return obj.isoformat()

        if isinstance(obj, date):
            return str(obj)
        if isinstance(obj,  RowMapping):
            #print(dict(obj))
            return dict(obj)

        return json.JSONEncoder.default(self, obj)

def post(data,end_point,header=None ,timeout=None):
        if not header:
            REQUESTS_HEADER = {'Content-type': 'application/json','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36','Connection': 'Close'}
        else:
            REQUESTS_HEADER=header
        #'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
        if not timeout:
            timeout=91

        if(type(data)==list):
            for row in data:
                #data=dict(data)
                r = requests.post(url = end_point, data = data,headers=REQUESTS_HEADER,verify=False,timeout=timeout)
                r.raw.chunked = True # Fix issue 1
                r.encoding = 'utf-8'

        else:
            req=requests.session()
            #data=dict(data)
            r = req.post(url = end_point, data = data,headers=REQUESTS_HEADER,verify=False,timeout=timeout)
            r.raw.chunked = True # Fix issue 1
            r.encoding = 'utf-8'

        return r

def get(data,end_point,header=None,timeout=None):

        if not header:
            REQUESTS_HEADER = {'Content-type': 'application/json','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36','Connection': 'Close'}
        else:
            REQUESTS_HEADER=header
        #'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
        if not timeout:
            timeout=91

        if(type(data)==list):
            for row in data:

                r = requests.get(url = end_point, headers=REQUESTS_HEADER,verify=False,timeout=timeout)
                r.raw.chunked = True # Fix issue 1
                r.encoding = 'utf-8'

        else:
            r = requests.get(url = end_point, headers=REQUESTS_HEADER,verify=False,timeout=timeout)
            r.raw.chunked = True # Fix issue 1
            r.encoding = 'utf-8'

        return r

def to_json(data,key=None):
    ''' Return results as Valid JSON  object, if Key is provided
    return each item has a dict whit Key=Key and Value=Json String '''
    data_json=[]
    #data=data
    for row in data:
        #print(type(row))
        json_str=json.dumps(row,cls=CustomJsonEncoder)
        if key is not None:
            json_key=row[key]
            data_json.append(dict(key=json_key,string=json_str))
        else:
            data_json.append(json_str)
        #print(data_json)
    return data_json 