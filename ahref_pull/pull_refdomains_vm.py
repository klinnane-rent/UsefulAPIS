import requests
import json
from datetime import datetime
from google.cloud import bigquery
import io
import pandas as pd 
import time
from pandas.io.json import json_normalize

def initial_pull(domain, table_id, table_schema, schema, request, path_append=None, send_it=False):
    # queries already made
    base_request = request
    if send_it:
        print('retreiving old data from : {}'.format(domain))
        request = base_request.format(domain)
        response = requests.get(request)
        #print(response.json())
    
        write_json(domain, response.json())
        format_jsons(domain)
        json_to_bq(domain, table_id, table_schema, schema, path_append)
    else:   
        print('we got the historics.')

def initial_pull(domain, request, path_append=None, send_it=False):
    base_request = request
    if send_it:
        print('retreiving old data from : {}'.format(domain))
        request = base_request.format(domain)
        response = requests.get(request)
        #print(response.json())
        fp = path_append + "_" + domain 
        write_json(fp, response.json())
        #format_jsons(domain)
    else:   
        request = base_request.format(domain)
        response = requests.get(request)
        #print(response.json())
        fp = path_append + "_" + domain  + '.json'
        #ctData= json.loads(jsonData)
        #print(response.json())
        print(response.json())
        write_json(response.json(), fp   )
        
        #write_json(fp, response.json())
        print('we got the historics.')

        
def write_json(fp, data):
    with open('{}.json'.format(fp), 'w') as f:
        json.dump(data, f)
    
def pull_ahref_to(fromdate, domain, request):
    base_request = request 

    request = base_request.format(domain, str(fromdate.date()))
    response = requests.get(request)
    new_fp = '{}'.format(domain)
    
    append_to_json(response.json() , new_fp )
        
def format_schema(schema):
    formatted_schema = []
    for row in schema:
        if 'fields' not in row.keys():
            formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))   
        else:
            formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))   
            formatted_schema2 = []
            for row2 in row['fields']:
                formatted_schema2.append(bigquery.SchemaField(row2['name'], row2['type'], row2['mode']))   
            formatted_schema.append(formatted_schema2)
    return formatted_schema       
def json_to_bq(domain, table_id, table_schema, schema, path_append=None, series  = False):

    bq_client = bigquery.Client.from_service_account_json("bq_consumerproductpython_cred.json")


                
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
 
    if path_append:
        path = domain + path_append +'.json'
    else:
        path = domain + ".json"

    
    if series:
        ser = pd.read_json(path, typ='series', convert_dates=False)
        df2 = pd.DataFrame([ser])
        if 'date' in df2.columns:
            df2['date'] = pd.to_datetime(df2['date'])
        bq_id = table_id.split(".")[0]
        other_id = ".".join(table_id.split(".")[1:])
        df2.to_gbq(other_id, bq_id,  if_exists='append', table_schema=table_schema )
    else:
        myList = []
        with open(path) as data_file:    
            for jsonObj in data_file:
                studentDict = json.loads(jsonObj)
                # removing repeated record option
                if 'refpages' in studentDict:
                    myList = studentDict['refpages']
                elif 'refdomains' in studentDict:
                    myList = studentDict['refdomains']
        
        df = json_normalize(myList)
       
        #df = pd.read_json(path, orient='records', lines=False, dtype=True)
        df['target'] = domain
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        if 'first_seen' in df.columns:
            df['first_seen'] = pd.to_datetime(df['first_seen'])
        if 'last_seen' in df.columns:
            df['last_seen'] = pd.to_datetime(df['last_seen'])
        
        #df2 = df.to_json(orient = 'records', lines=True)
        #if 'date' in df2.columns:
        #    df2['date'] = pd.to_datetime(df2['date'], unit='D')
        #if 'first_seen' in df2.columns:
        #    df2['first_seen'] = pd.to_datetime(df2['first_seen'])
        print(df.info())
        """
        try:
            bq_id = table_id.split(".")[0]
            other_id = ".".join(table_id.split(".")[1:])
            
            df.to_gbq(other_id, bq_id,  if_exists='append', table_schema=table_schema )
        except Exception as e: 
            print(e)
            print("Error uploading {} to bq.  Most likely empty json.".format(domain))
        """
def format_jsons(domain,path_append = None,list_of_dict_to_append = None):

    # uncomment once complete
    if path_append:
        path = domain + path_append
    else:
        path = domain
    if list_of_dict_to_append:
        added_dict = []
        myList = [{'target': domain}] + list_of_dict_to_append
    else:
        myList = [{'target': domain}]
   
    append_to_json(myList, "{}.json".format(path))
       
# appends attribute to json
def append_to_json(_dict,path): 
    with open(path, 'ab+') as f:
        f.seek(0,2)                                #Go to the end of file    
        if f.tell() == 0 :                         #Check if file is empty
            f.write(json.dumps([_dict]).encode())  #If empty, write an array
        else :
            f.seek(-2,2)           
            f.truncate()      
            #print(_dict.keys())
            for aDict in _dict['refdomains']:                   #Remove the last character, open the array
                f.write(' , '.encode()) 
                print(list(aDict.keys())[0])#Write the separator
                #p_dict = _dict[aDict]
                #print(len(p_dict))
                f.write('{'.encode())
                for i, val in enumerate(aDict):
                    
                    if i == len(aDict)-1:
                        f.write('"{}": "{}"'.format(val,aDict[val] ).encode())
                    else:
                        
                        f.write('"{}": "{}",'.format(val,aDict[val] ).encode())    #Dump the dictionary

                    
                f.write('}'.encode())     
                #Close the array
        f.write(']'.encode())
        f.write('}'.encode())
        #f.write(']'.encode())
            
# gets domain rating for the day.
def get_dr(domains):
    base_request = "https://apiv2.ahrefs.com?token=5a94c007693f438ddf4bcd41c4f304ece2df0b8c&target={}.com&limit=1000&output=json&from=domain_rating&mode=subdomains"
    table_schema = [
               
                      {'name': 'domain_rating', 'type': 'INTEGER'},
                      {'name': 'ahrefs_top', 'type': 'INTEGER'},
                      {"name": 'target', 'type' : 'STRING', 'mode': 'REQUIRED'},
                      {'name': 'date', 'type': "DATETIME"}
                      
                  
                  ]
    table_id='big-query-152314.klinnane.Ahref_compteitive_DR'
    schema = [
        
            bigquery.SchemaField('domain_rating', 'INTEGER'),
            bigquery.SchemaField('ahrefs_top', 'INTEGER'),
            bigquery.SchemaField('target', 'STRING', mode='REQUIRED'), 
            bigquery.SchemaField('date', 'DATETIME')
        
              ]
    
    todays_date = datetime.today()
    for domain in domains:
        request = base_request.format(domain)
        response = requests.get(request)
        #print(response.json())
        try:
            write_json(domain +"_dr", response.json()['domain'])
            format_jsons(domain, "_dr", [{'date': todays_date}])
            json_to_bq(domain, table_id, table_schema, schema, '_dr', True)
        except Exception as e:
            time.sleep(60)

            response = requests.get(request)
            try:
                write_json(domain +"_dr", response.json()['domain'])
                format_jsons(domain, "_dr", [{'date': todays_date}])
                json_to_bq(domain, table_id, table_schema, schema, '_dr', True)
            except Exception as e2:
                pass


def getBackLinkData(domains,retrieve_historics):
    table_schema =     [
        {
        "name": "date",
        "type": "DATETIME"
        },
        {
        "name": "type",
        "type": "STRING"
        },
        {
                "name": "domain_rating",
                "type": "INTEGER"
                
        },
        {
            'name': 'url_from',
             'type': 'STRING'
        },
 
        {
            'name': 'url_to',
             'type': 'STRING'
        },



        {
            'name': 'first_seen',
             'type': 'DATETIME'
        },
        {
        "name": "target",
        "type": "STRING"    
        }
                
            
    ]
    schema = [
            bigquery.SchemaField('date', 'DATETIME'),
            bigquery.SchemaField('type', 'STRING' ),
            bigquery.SchemaField('domain_rating', 'INTEGER'),
            bigquery.SchemaField('url_from', 'STRING'),
            bigquery.SchemaField('url_to', 'STRING'),
            bigquery.SchemaField('first_seen', 'DATETIME'),
            bigquery.SchemaField('target', 'STRING')
   
            
            
            
            
            
            
            
            
              ]
    table_id = 'big-query-152314.klinnane.Ahref_backlinks'


    path_append = ""
    last_pulled = ""

    today = datetime.today()
    with open("last_pulled_backlinks.txt", 'r') as f:
        last_pulled = f.readline().rstrip()
        f.close()
    last_pulled = datetime.strptime(last_pulled, '%Y-%m-%d')
    print(str(last_pulled.date()))

    for domain in domains:
        if last_pulled.date() != today.date() or retrieve_historics:
            if retrieve_historics:
                request = 'https://apiv2.ahrefs.com?token=5a94c007693f438ddf4bcd41c4f304ece2df0b8c&target={}.com&output=json&from=backlinks_new_lost&mode=subdomains&where=date%3E%3D%222013-12-02%22&limit=10000000&having=substring(url_to%2C%22%2Fblog%2F%22)&select=date,type,domain_rating,url_from,url_to,first_seen'
                initial_pull(domain, table_id,  table_schema, schema, request, path_append, retrieve_historics)
            else:
                request = 'https://apiv2.ahrefs.com?token=5a94c007693f438ddf4bcd41c4f304ece2df0b8c&target={}.com&output=json&from=backlinks_new_lost&mode=subdomains&where=date%3E%22{}%22&limit=10000000&having=substring(url_to%2C%22%2Fblog%2F%22)&select=date,type,domain_rating,url_from,url_to,first_seen'
                pull_ahref_to(last_pulled, domain, request)
    
           
                format_jsons(domain)
                json_to_bq(domain, table_id, table_schema, schema, path_append)
        else:
            print('all updated')
       
    with open("last_pulled_backlinks.txt", 'w') as f:
        f.write(str(today.date().strftime("%Y-%m-%d")))
        


def getReferringDomains(domains, retrieve_historics):

    table_schema =     [{
        "name": "target",
        "type": "STRING",
        "mode": "REQUIRED"
    },
        
            {
                "name": "first_seen",
                "type": "DATETIME"
                
            },
            {'name': 'refdomain', 'type':'STRING'},
            {'name':'backlinks', 'type':'INTEGER'},
            {'name': 'refpages', 'type':'INTEGER'},
            {'name':'last_visited', 'type': 'DATETIME'},
            {'name':'domain_rating', 'type' :'INTEGER'}
    ]
    schema = [
        bigquery.SchemaField('target', 'STRING', mode='REQUIRED'), 
            bigquery.SchemaField('first_seen', 'DATETIME', mode='REQUIRED'),
            bigquery.SchemaField('refdomain', 'STRING'),
            bigquery.SchemaField('backlinks', 'INTEGER'),
           
            bigquery.SchemaField('refpages', 'INTEGER'),
            
            bigquery.SchemaField('last_visited', 'DATETIME'),
            bigquery.SchemaField('domain_rating', 'INTEGER')
              ]
    table_id = 'big-query-152314.klinnane.Ahref_refferring_domains'
    # code block should be true when a new domain is used. But only that domain should pass through this method, 

    
    
    
    path_append = ""
    last_pulled = ""

    today = datetime.today()
    with open("last_pulled_refdomains.txt", 'r') as f:
        last_pulled = f.readline().rstrip()
        f.close()
    last_pulled = datetime.strptime(last_pulled, '%Y-%m-%d')
    print(str(last_pulled))
    for domain in domains:
        print(domain)
        if last_pulled.date() != today.date() or retrieve_historics:
            if retrieve_historics:
                request = "https://apiv2.ahrefs.com?token=5a94c007693f438ddf4bcd41c4f304ece2df0b8c&target={}.com&limit=1000000&output=json&from=refdomains&mode=subdomains&where=last_visited%3E%3D%222013-12-02%22&&having=domain_rating%3E%3D20"
                initial_pull(domain, table_id, table_schema, schema, request, path_append, retrieve_historics)
            else:
                request = "https://apiv2.ahrefs.com?token=5a94c007693f438ddf4bcd41c4f304ece2df0b8c&target={}.com&limit=1000000&output=json&from=refdomains&mode=subdomains&where=last_visited%3E%3D%22{}%22"
                print("Pulling ahref data")
                pull_ahref_to(last_pulled, domain, request)
    
                print("formatting ahref to json")
                format_jsons(domain)
                print("Json to bq")
                json_to_bq(domain, table_id, table_schema, schema, path_append)
                
        else:
            print('all updated')
    with open("last_pulled_refdomains.txt", 'w') as f:
        f.write(str(today.date().strftime("%Y-%m-%d")))

def getNewLost(domains, retrieve_historics):
    path_append = "new_lost_temp"
    today = datetime.today()
    last_pulled = ""
    with open("last_pulled.txt", 'r') as f:
        last_pulled = f.readline().rstrip()
    #last_pulled = datetime.strptime(last_pulled, '%Y-%m-%d')
    if last_pulled.date() != today.date():
        date_after_equal = "2023-10-03"
        for domain in domains:
            if retrieve_historics:
                request = "https://apiv2.ahrefs.com?token=5a94c007693f438ddf4bcd41c4f304ece2df0b8c&target={}.com&limit=1000000000&output=json&from=refdomains_new_lost&mode=subdomains&having=domain_rating%3E%3D20"
                initial_pull( domain, request, path_append, retrieve_historics)
            else:
                request = "https://apiv2.ahrefs.com?token=5a94c007693f438ddf4bcd41c4f304ece2df0b8c&target={}.com&limit=1000000000&output=json&from=refdomains_new_lost&mode=subdomains&having=domain_rating%3E%3D20&where=date%3E%3D%22{}%22".format(domain, last_pulled)
                initial_pull( domain, request, path_append, retrieve_historics)
        

# TODO: Continiously store to big query.
if __name__ == '__main__':
    #domains = [  'trulia', 'rentcafe', 'rent', 'apartments', 'rentals', 'forrent','zillow', 'apartmentguide', 'hotpads']
    domains = [ 'rent', 'rentals', 'apartmentguide', 'rentcafe', 'apartments', 'forrent','zillow',  'hotpads', 'redfin', 'apartmentlist', 'realtor']
    #domains = ['rent']
    # TODO: RUN THIS WHEN MORE AHREF CREDITS
    #blog_domains = [  'rentals','rent','apartmentguide']
    # estimated 428k row cost
    #getBackLinkData(blog_domains, False)
    # estimated 
    getNewLost(domains, False)
    #get_dr(domains)