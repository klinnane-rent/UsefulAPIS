import os, sys
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np
import random
from datetime import datetime

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL  # Snowflake interface with sqlalchemy
import json
from pprint import pprint
import snowflake.connector
import create_bq_table

pwd = os.path.dirname(os.path.abspath(__file__))
keyfile = '/Users/rentpath/Documents/bigquery_info/bq_consumerproductpython_cred.json'  # for reading GA4SessionAttribution in BigQuery 

stream_ids = {
    'AG': 2712637656,
    'Rent': 2119933141,
}

#start, end = 20240513, 20240528
start, end = 20240807, 20240820
#start, end = 20240611, 20240611
snow_tmpl_file = f'{pwd}/snow_DS542.TMP.sql' # Snowflake SQL code template (for brand name and start-end dates)


"""
GA4 data collection process for DS-464 experiment Tableau (Snowflake 'GA4_EVENT' + BQ 'GA4SessionAttribution').
Another major devidation from old UA-based code is the session (vs city) granularity.

1. collect AG
1.1. Snowflake query for AG data
* customize 'snow_DS452.TMPL.sql' for AG for the date range;
* run the SQL code against Snowflake with svc acct;
1.2. retrieve 'GA4SessionAttribution_AG' from BigQuery for session attributes (channel, bounce, duration, etc)
1.3. inner-join with ['date', 'sessionid']
(Rent not needed for the AG OpenSearch re-run)

2. repeat step-1 for Rent, and combine with AG.

3. upload to BQ 'DS452GA4'
"""

def query_snowflake(q):
    engine = create_engine(URL(
        account = 'ue67447.east-us-2.azure',
        user = '',
        password = '',
        database = 'EDW',
        schema = 'WEB_ACTIVITY',
        warehouse = 'SNOWFLAKE_DATA_SCIENCE_WH',
        role='SNOWFLAKE_DATA_SCIENCE_USERS',
    ))
    print(engine)
    print(q)
    df = pd.read_sql(q, engine)
    engine.dispose()
    print(df.info())
    return df


def query_experiment_data(brand, start, end):
    sql = open(snow_tmpl_file).read()
    subs = {
        '{brand}': brand,
        '{brand_stream_id}': stream_ids[brand],
        '{start_date}': start,
        '{end_date}': end,
    }
    for k, v in subs.items():
        sql = sql.replace(k, str(v))
    #print(sql)  # for debugging
    fname = f'{pwd}/query_experiment_data.{brand}.{start}-{end}.sql'  # the Snowflake SQL code, for one brand
    open(fname, 'w').write(sql)
    print(f'Wrote Snowflake SQL code into {fname}.')

    df = query_snowflake(sql)
    return df


def fetch_GA4_sessionized(brand, start, end):  # retrieve from BigQuery pre-computed GA4 session tables
    import google.cloud.bigquery as bq
    project = 'big-query-152314'
    bq_datasets = {
        'AG': 'GA4SessionAttribution_AG',
        'Rent': 'GA4SessionAttribution_Rent',
    }

    dataset = bq_datasets[brand]
    table = f'{brand}_*'
    fullpath = f'{project}.{dataset}.{table}'
    sql = f"""
        select PARSE_DATE("%Y%m%d", cast(event_date as string)) as date
        ,session_id as sessionid
        ,duration as time_on_site
        ,engaged
        ,case engaged
            when 1 then 0
            else 1
            end as bounces2
        ,case cg4_lastnd
            when 'Paid Search' then 'Paid'
            when 'Display' then 'Paid'
            when 'Email' then 'Paid'
            else 'Organic'
            end as Channel
        from `{fullpath}` where _TABLE_SUFFIX between '{start}' and '{end}'
"""

    client = bq.Client.from_service_account_json(keyfile)
    print(sql)
    df = client.query(sql).to_dataframe()
    print(df.dtypes)
    print(df.shape)
    print(df.head())
    return df


def get_by_brand(brand, start, end):
    sess = fetch_GA4_sessionized(brand, start, end)
    df = query_experiment_data(brand, start, end)
    df = df.merge(sess, on=['date', 'sessionid'])
    return df


def get_all(start, end):
    ag = get_by_brand('Rent', start, end)
    #rent = get_by_brand('Rent', start, end)
    df = ag
    return df
    

###########################################
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f'>>>>>> {__file__} started at "%s" >>>>>>'%now)

#brand = 'AG'
#brand = 'Rent'
#df = get_by_brand(brand, start, end)
#df.to_pickle(f'get_by_brand.{brand}.{start}-{end}.pkl')

df = get_all(start, end)
fname = f'{pwd}/get_all.{start}-{end}.pkl'
df.to_pickle(fname)
print(f'Wrote {fname}.')

bq_output = 'DS542'
create_bq_table.store_bq_DS542(df, bq_output)
print(f'Uploaded to {bq_output}.')

now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f'<<<<<< {__file__} ended at "%s" <<<<<<'%now)
