import os, sys
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np
import random
from datetime import datetime

import pymssql
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL  # Snowflake interface with sqlalchemy
import json
from pprint import pprint

import upload_bq_DS545

pwd = os.path.dirname(os.path.abspath(__file__))
keyfile = '/Users/rentpath/Documents/bigquery_info/bq_consumerproductpython_cred.json'  # for reading GA4SessionAttribution in BigQuery 

stream_ids = {
    'AG': 2712637656,
    'Rent': 2119933141,
}

start, end = 20240829, 20240911
snow_tmpl_file = f'{pwd}/snow_DS545.TMPL.sql' # Snowflake SQL code template (for brand name and start-end dates)


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

(2). upload to BQ 'DS452GA4'
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
    ag = get_by_brand('AG', start, end)
    #rent = get_by_brand('Rent', start, end)
    #df = pd.concat([ag, rent])
    #return df
    return ag


def add_tier(df, start, end):
    conn = pymssql.connect(server='datascience.corp.primedia.com', user='reader', password='R3ader1!')
    q = f"select ymdid as date, listingid, PlacementTierAG as tier from warehouse.rentpath.mart.listings where ymdid between '{start}' and '{end}' and statusidag=1"
    print(q)
    tier = pd.read_sql(q, conn)
    conn.close()
    print(tier.shape)

    tier['tier'] = np.where(tier.tier==40, 30, tier.tier)  # silver-lite to silver
    tier['tier'] = np.where(~tier.tier.isin({5, 10, 20, 30}), 20, tier.tier)  # default to gold
    tier_map = {5: 'Diamond', 10: 'Platinum', 20: 'Gold', 30: 'Silver'}
    tier['tier'] = tier.tier.apply(lambda x: tier_map[x])
    tier.to_pickle('tier.pkl')

    tier['date'] = tier.date.astype(str).apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}')
    tier['listingid'] = tier.listingid.astype(str)

    def agg_tier(df, tier):
        df = df[df['core_lids'].notna()]
        df = df[df['core_lids']!='']
        df['listingid'] = df.core_lids.apply(lambda x: set(x.split(',')))
        df = df.explode('listingid')
        df['date'] = df['date'].astype(str)
        print(df.head())
        df = df.merge(tier, on=['date', 'listingid'], how='left').fillna('Gold')

        res = df.groupby(['date', 'sessionid', 'variant', 'tier']).size().to_frame('occ').reset_index()
        res = res.pivot(index=['date', 'sessionid', 'variant'], values='occ', columns='tier').fillna(0).reset_index()
        return res

    res = agg_tier(df, tier)
    res.to_pickle('tiered.pkl')
    res['date'] = pd.to_datetime(res['date']).dt.date
    df = df.merge(res, how='left', on=['date', 'sessionid', 'variant'])
    return df


def add_pin(df, start, end):
    conn = pymssql.connect(server='datascience.corp.primedia.com', user='reader', password='R3ader1!')
    q = f"select ymdid as date, listingid, nv_rpl as rpl, pindecile from Production.pin.pin where ymdid between '{start}' and '{end}'"
    print(q)
    pin = pd.read_sql(q, conn)
    conn.close()
    print(pin.shape)

    pin['pin_bucket'] = pin.pindecile.apply(lambda x: 'pin_1_3' if x<=3 else ('pin_4_7' if x in (4, 5, 7) else 'pin_8_10'))
    pin['date'] = pin.date.astype(str).apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}')
    pin['listingid'] = pin.listingid.astype(str)
    pin.to_pickle(f'pin.pkl.{start}-{end}')

    def agg_rpl(df, pin):
        df = df[df['core_lids'].notna()]
        df = df[df['core_lids']!='']
        df['listingid'] = df.core_lids.apply(lambda x: set(x.split(',')))
        df = df.explode('listingid')
        df['date'] = df['date'].astype(str)
        print(df.head())
        df = df.merge(pin, on=['date', 'listingid'], how='left').fillna('pin_1_3')

        #res = df.groupby(['date', 'sessionid', 'variant'])['rpl'].sum().to_frame('session_rpl').reset_index()
        #res['date'] = pd.to_datetime(res['date']).dt.date
        #res = res[['date', 'sessionid', 'variant', 'session_rpl']]
        #print('matched:', res.shape, res.dtypes)

        res = df.groupby(['date', 'sessionid', 'variant', 'pin_bucket']).size().to_frame('occ').reset_index()
        res = res.pivot(index=['date', 'sessionid', 'variant'], values='occ', columns='pin_bucket').fillna(0).reset_index()
        return res

    res = agg_rpl(df, pin)
    res.to_pickle('pin_bucket.pkl')
    res['date'] = pd.to_datetime(res['date']).dt.date
    df = df.merge(res, how='left', on=['date', 'sessionid', 'variant'])
    #df['lead_rpl'] = df['session_rpl']/df['core_total_leads']
    return df
    

###########################################
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f'>>>>>> {__file__} started at "%s" >>>>>>'%now)

df = get_all(start, end)
fname = f'{pwd}/get_all.{start}-{end}.pkl'
df.to_pickle(fname)
print(f'Wrote {fname}.')

df = add_pin(df, start, end)
fname = f'{pwd}/get_all_pin.{start}-{end}.pkl'
df.to_pickle(fname)
print(f'Wrote {fname}.')

df = add_tier(df, start, end)
fname = f'{pwd}/get_all_pin_tier.{start}-{end}.pkl'
df.to_pickle(fname)
print(f'Wrote {fname}.')

bq_output = 'DS545'
upload_bq_DS545.store_bq(df, bq_output)
print(f'Uploaded to {bq_output}.')

now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f'<<<<<< {__file__} ended at "%s" <<<<<<'%now)
