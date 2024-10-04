#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 14:23:17 2022

@author: rentpath
"""

from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
import urllib.request, urllib.parse, urllib.error
import httplib2
from xml.dom import minidom
import requests 
from requests.auth import HTTPBasicAuth
import json
from bs4 import BeautifulSoup 
import datetime
import pandas as pd 
import time
import numpy as np 
import json
import collections
from time import sleep
import time 
baseurl = 'https://splunk-search002.useast2.rentpath.com:8089'
userName = '###'
port = 8089
host = 'splunk-search002.useast2.rentpath.com'
password = '###'
import splunklib.results as results

import sys
#sys.path.insert(0, '/Users/rentpath/Documents/SEO/botify_to_splunk_regex/')

from format_pagetypes import page_rules
import re
#searchQuery = '| inputcsv foo.csv | where sourcetype=access_common | head 5'
pageRules = page_rules()
def df_to_bq(df, schema, table):
    for i, schem  in enumerate(schema):
        if schem['name'] in df.columns:
            if schem['type'] == 'INTEGER':
                df[schem['name']] = df[schem['name']].astype(pd.Int32Dtype())
            elif schem['type'] == 'DATE':
                df[schem['name']] =  pd.to_datetime(df[schem['name']])
            elif schem['type'] == 'BOOLEAN':
                df[schem['name']] = df[schem['name']].astype('boolean')
            elif schem['type'] == 'DATETIME':
                #df[schema['name']] = df[schema['name']].astype('boolean')
                df[schem['name']] =  df[schem['name']].astype('datetime64[ns]')
    
    try:
        print(df)
        
        if 'www.rent.com' in df['ClientRequestHost'].iloc[0]:
            #vals = df[['ClientRequestHost','ClientRequestURI']].agg(''.join, axis=1)
            df['label'] = df['ClientRequestURI'].apply(pageRules.create_labels,site='rent')
        elif 'rentals.com' in df['ClientRequestHost'].iloc[0]:
            #vals = df[['ClientRequestHost','ClientRequestURI']].agg(''.join, axis=1)
            df['label'] = df['ClientRequestURI'].apply(pageRules.create_labels,site='rentals')
        elif 'apartmentguide.com' in df['ClientRequestHost'].iloc[0]:
            df['label'] = df['ClientRequestURI'].apply(pageRules.create_labels,site='ag')
        elif 'solutions.rent.com' in df['ClientRequestHost'].iloc[0]:
            #vals = df[['ClientRequestHost','ClientRequestURI']].agg(''.join, axis=1)
            df['label'] = df['ClientRequestURI'].apply(pageRules.create_labels,site='rent')
        df['label'] = ""
        df.to_gbq(table, 'big-query-152314',  if_exists='append', table_schema=schema )
        print('uplodaded to: {}'.format(table))
    except Exception as e:
        print('failed upload to : {}'.format(table))
        print(e)
        
def executeQuery(searchQuery):
    # Authenticate with server.
    # Disable SSL cert validation. Splunk certs are self-signed.
    serverContent = httplib2.Http(disable_ssl_certificate_validation=True).request(baseurl + '/services/auth/login',
        'POST', headers={}, body=urllib.parse.urlencode({'username':userName, 'password':password}))[1]
    
    sessionKey = minidom.parseString(serverContent).getElementsByTagName('sessionKey')[0].childNodes[0].nodeValue
    # Remove leading and trailing whitespace from the search
    searchQuery = searchQuery.strip()

    # If the query doesn't already start with the 'search' operator or another
    # generating command (e.g. "| inputcsv"), then prepend "search " to it.
    if not (searchQuery.startswith('search') or searchQuery.startswith("|")):
        searchQuery = 'search ' + searchQuery
        print(searchQuery)

    # Run the search.
    # Again, disable SSL cert validation.
    jobId = httplib2.Http(disable_ssl_certificate_validation=True).request(baseurl + '/services/search/jobs','POST',
        headers={'Authorization': 'Splunk %s' % sessionKey},body=urllib.parse.urlencode({'search': searchQuery}))[1]
    jobId = str(jobId)
    print(jobId)
    try :
        jobz = re.search("<sid>(.*\..*)</sid>", jobId).group(1)
        
    except Exception as e:
        print('No job id returned.')
        print(e)
    jobz = jobz.strip()
    try:
        jobz = float(jobz)
    except Exception as e:
        print('Job cannot be covnerted to float.')
    print(jobz)
    return jobz
def response_to_bq(content):

    timeRan = datetime.datetime.now() - datetime.timedelta(minutes = 30)
    print(timeRan)
    dataframe = pd.DataFrame(columns=['Date','ClientRequestURI' , 'BotScore', 'ClientRequestUserAgent', 'ClientRequestReferer', 'ClientRequestHost', 'EdgeResponseStatus'])
    for i in content:
        # 
        botScore = np.nan
        ClientRequestURI = ""
        ClientRequestUserAgent = ""
        ClientRequestReferer = ""
        ClientRequestHost = ""
        EdgeResponseStatus  = np.nan
        for key, val in i.items():
            if isinstance(val, list):
                val = val[0]
            
            if key == '_raw':
                #decoder = json.JSONDecoder()
                b =  json.loads(i['_raw'])
          
                for key in b.keys():
                   
                    if isinstance(b[key], list) and b[key]:
                        b[key] = b[key][0]
                    if 'BotScore' == key:
                       botScore = b[key]
        
                    if 'ClientRequestURI' == key:
                        ClientRequestURI = b[key]
        
                    if 'ClientRequestUserAgent' ==  key:
                        ClientRequestUserAgent = b[key]
        
                    if 'ClientRequestReferer' == key:
                        ClientRequestReferer = b[key]
        
                    if 'ClientRequestHost' == key:
                        ClientRequestHost = b[key]
         
                    if 'EdgeResponseStatus' == key:
                        EdgeResponseStatus = b[key]
            if 'BotScore' == key:
               botScore = val

            if 'ClientRequestURI' == key:
                ClientRequestURI = val

            if 'ClientRequestUserAgent' ==  key:
                ClientRequestUserAgent = val

            if 'ClientRequestReferer' == key:
                ClientRequestReferer = val

            if 'ClientRequestHost' == key:
                ClientRequestHost = val
 
            if 'EdgeResponseStatus' == key:
                EdgeResponseStatus = val
        dataframe.loc[len(dataframe)] = [timeRan, ClientRequestURI, botScore,  ClientRequestUserAgent, ClientRequestReferer, 
                                         ClientRequestHost, EdgeResponseStatus] 
    print(dataframe)
    table_schema = [
        {'name': 'Date', 'type': 'DATETIME'},
        {'name' :'ClientRequestURI', 'type': 'STRING' },
        {'name': 'BotScore', 'type': 'INTEGER' },
        {'name': 'ClientRequestUserAgent', 'type': 'STRING'},
        {'name': 'ClientRequestReferer', 'type': 'STRING'},
        {'name': 'ClientRequestHost', 'type': 'STRING'},
        {'name': 'EdgeResponseStatus', 'type': 'INTEGER'},
        {'name': 'label', 'type': 'STRING'}
        ]
    table = 'klinnane.RentPath_HTTP_Status'
  
    #table = table.format(timeRan.strfrmt("%Y%m%d"))
    df_to_bq(dataframe,table_schema, table)




if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
    import splunklib.client as client
    import splunklib.results as results
    timeout = time.time() + 60*10
    print('--Rentals--')
    service = client.connect(
    host=host,
    port=port, 
    username=userName,
    password=password)
    full_result = []
    job = service.jobs.create('search index=cloudflare ClientRequestHost = "www.rentals.com" ClientRequestMethod = GET  ClientRequestUserAgent=*googlebot* earliest=-1h@h latest=@h  | regex ClientRequestPath !=".js$|.png$|\.[A-z]+$|.woff2$"')
    while True:
        while not job.is_ready():
            pass
        stats = {"isDone": job["isDone"],
                 "doneProgress": float(job["doneProgress"])*100,
                  "scanCount": int(job["scanCount"]),
                  "eventCount": int(job["eventCount"]),
                  "resultCount": int(job["resultCount"])}

        status = ("\r%(doneProgress)03.1f%%   %(scanCount)d scanned   "
                  "%(eventCount)d matched   %(resultCount)d results") % stats

        sys.stdout.write(status)
        sys.stdout.flush()
        if stats["isDone"] == "1":
            sys.stdout.write("\n\nDone!\n\n")
            break
        sleep(2)

    # Get the results and display them
    for result in results.ResultsReader(job.results(count=0)):
         full_result.append(result)
    job.cancel()   
    response_to_bq(full_result)
    
    full_result = []
    job = service.jobs.create('search index=cloudflare ClientRequestHost = "www.apartmentguide.com" ClientRequestMethod = GET  ClientRequestUserAgent=*googlebot* earliest=-1h@h latest=@h  | regex ClientRequestPath !=".js$|.png$|\.[A-z]+$|.woff2$"')
    while True:
        while not job.is_ready():
            pass
        stats = {"isDone": job["isDone"],
                 "doneProgress": float(job["doneProgress"])*100,
                  "scanCount": int(job["scanCount"]),
                  "eventCount": int(job["eventCount"]),
                  "resultCount": int(job["resultCount"])}

        status = ("\r%(doneProgress)03.1f%%   %(scanCount)d scanned   "
                  "%(eventCount)d matched   %(resultCount)d results") % stats

        sys.stdout.write(status)
        sys.stdout.flush()
        if stats["isDone"] == "1":
            sys.stdout.write("\n\nDone!\n\n")
            break
        sleep(2)

    # Get the results and display them
    for result in results.ResultsReader(job.results(count=0)):
         full_result.append(result)
    job.cancel()   
    response_to_bq(full_result)
    
    
    full_result = []
    job = service.jobs.create('search index=cloudflare ClientRequestHost = "www.rent.com" ClientRequestMethod = GET  ClientRequestUserAgent=*googlebot* earliest=-1h@h latest=@h  | regex ClientRequestPath !=".js$|.png$|\.[A-z]+$|.woff2$"')
    while True:
        while not job.is_ready():
            pass
        stats = {"isDone": job["isDone"],
                 "doneProgress": float(job["doneProgress"])*100,
                  "scanCount": int(job["scanCount"]),
                  "eventCount": int(job["eventCount"]),
                  "resultCount": int(job["resultCount"])}

        status = ("\r%(doneProgress)03.1f%%   %(scanCount)d scanned   "
                  "%(eventCount)d matched   %(resultCount)d results") % stats

        sys.stdout.write(status)
        sys.stdout.flush()
        if stats["isDone"] == "1":
            sys.stdout.write("\n\nDone!\n\n")
            break
        sleep(2)

    # Get the results and display them
    for result in results.ResultsReader(job.results(count=0)):
         full_result.append(result)
    job.cancel()   
    response_to_bq(full_result)
    
    
    
    full_result = []
    job = service.jobs.create('search index=cloudflare ClientRequestHost = "solutions.rent.com" ClientRequestMethod = GET  ClientRequestUserAgent=*googlebot* earliest=-1h@h latest=@h  | regex ClientRequestPath !=".js$|.png$|\.[A-z]+$|.woff2$"')
    while True:
        while not job.is_ready():
            pass
        stats = {"isDone": job["isDone"],
                 "doneProgress": float(job["doneProgress"])*100,
                  "scanCount": int(job["scanCount"]),
                  "eventCount": int(job["eventCount"]),
                  "resultCount": int(job["resultCount"])}

        status = ("\r%(doneProgress)03.1f%%   %(scanCount)d scanned   "
                  "%(eventCount)d matched   %(resultCount)d results") % stats

        sys.stdout.write(status)
        sys.stdout.flush()
        if stats["isDone"] == "1":
            sys.stdout.write("\n\nDone!\n\n")
            break
        sleep(2)

    # Get the results and display them
    for result in results.ResultsReader(job.results(count=0)):
         full_result.append(result)
    job.cancel()   
    response_to_bq(full_result)
