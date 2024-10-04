import os, sys
from datetime import timedelta, datetime

import pandas as pd
pd.set_option('display.width', 1000)
import numpy as np

#import google.cloud.bigquery as bq
from google.oauth2 import service_account
import pandas_gbq

#creds_file = '/home/dsvisitor/.ssh/big-query-152314-ad439efc179f.json'
creds_file = '/Users/rentpath/Documents/bigquery_info/bq_consumerproductpython_cred.json'
project = 'big-query-152314'
dataset = 'klinnane'

def store_bq_DS542(df, table):
    credentials = service_account.Credentials.from_service_account_file(creds_file)
    table_id = f'{dataset}.{table}'
    print(f'Writing {project}.{table_id} ...')

    schema = [{'name':'date','type': 'DATE'},
              {'name':'srp_pdp','type': 'FLOAT'},
              {'name':'srp_views','type': 'FLOAT'},
              {'name':'total_leads','type': 'FLOAT'},
              {'name':'total_leads_srp','type': 'FLOAT'},
              {'name':'core_total_leads','type': 'FLOAT'},
              {'name':'map_pin_click','type': 'FLOAT'},
              {'name':'srp_saves','type': 'FLOAT'},
              {'name':'srp_gallery','type': 'FLOAT'},
              {'name':'srp_engagements','type': 'FLOAT'},
              {'name':'bounces','type': 'FLOAT'},
              {'name':'time_on_site','type': 'FLOAT'}]
    res = pandas_gbq.to_gbq(df, table_id, project, credentials=credentials, if_exists='replace', table_schema=schema)
