import pytd
import os
import requests
import urllib3
import json
from collections import abc
import pandas as pd
import mapping as mp
import urllib.parse
import time

debug_level=int(os.environ['DSAPI_DEBUG_LEVEL'])

client_id=os.environ['DSAPI_CLIENT_ID']
client_secret=os.environ['DSAPI_CLIENT_SECRET']
oauth_endpoint=os.environ['DSAPI_OAUTH_ENDPOINT']
oauth_method=os.environ['DSAPI_OAUTH_METHOD']
oauth_grant_type=os.environ['DSAPI_OAUTH_GRANT_TYPE']
oauth_scope=os.environ['DSAPI_OAUTH_SCOPE']
oauth_username=os.environ['DSAPI_OAUTH_USERNAME']
oauth_password=os.environ['DSAPI_OAUTH_PASSWORD']
api_endpoint=os.environ['DSAPI_MATCH_ENDPOINT']
api_method=os.environ['DSAPI_MATCH_METHOD']
api_options=os.environ['DSAPI_MATCH_OPTIONS']
database_name=os.environ['DATABASE_NAME']
source_table=os.environ['SOURCE_TABLE']
dest_table=os.environ['DEST_TABLE']
dsapi_tenantid=os.environ['DSAPI_TENANTID']
dsapi_role=os.environ['DSAPI_ROLE']

global_oauth_token=None
global_db_client=None
global_dest_dict=[]
global_dest_df=None

def get_input_sql(table_name,limit):
  global debug_level
  # Generate from mapping.py the SQL select statement to get PII  
  sql='select '
  sql+=', '.join("{!s} as {!s}".format(val.replace('\n',''),key) for (key,val) in mp.source_mapping.items())
  sql+=f" from {table_name} "
  sql+=f" limit {limit}"
  
  if debug_level>1:
    print("Debug - Extract SQL generated from mapping.py: ",sql)
    
  return sql

def get_test_input_sql(table_name,limit):
   global debug_level
   # Force 1 record of test data   
   sql="select 1 as id, 'Jeremy' as firstName,'' as middleName,'Shaw' as lastName,'157 ARCHERHILL ROAD' as streetAddress,'GLASGOW' as city,'G13 3JQ' as zipCode from raw_synth_pii limit 1"
   
   if debug_level==9:
     print("Debug - Extract SQL forced to 1 record: ",sql)

   return sql

def read_source_pii(engine_name, limit):
    # Read PII from Source Table and return a Panda dataframe
    global database_name, source_table,global_db_client
    df="" 
    try:
        start_time = time.time()

        if debug_level<9:
           input_sql=get_input_sql(source_table,limit)
        else:
           input_sql=get_test_input_sql(source_table,limit)

        res = global_db_client.query(input_sql)
        df = pd.DataFrame(**res)
    except Exception as e:
        raise Exception("Exception reading source table - "+str(e))
    finally:   
        print("--- Total read "+str(len(df))+" source rows in : {:.3f} seconds ---".format((time.time() - start_time))) 
        return df

def read_results(limit,bundles_filter):
    # Read PII from DS-API output table and print pipe delimited in stdout
    global database_name, dest_table,global_db_client
    try:
        global_db_client=pytd.Client(database=database_name)
        bundle_where=""
        if bundles_filter:
            for bundle in bundles_filter.split(","):
                if bundle_where=="":
                    bundle_where=bundle_where+" where key like '%."+bundle+"%' "  
                else:  
                    bundle_where=bundle_where+" or key like '%."+bundle+"%' "  

        sql="select * from "+dest_table+bundle_where+" limit "+str(limit)
        print(sql)
        res = global_db_client.query(sql)

        print("ID|KEY|VALUE")
        for record in res["data"]:
            print(str(record[0])+"|"+record[1]+"|"+record[2])
 
    except Exception as e:
        raise Exception("Exception reading source table - "+str(e))
    finally:   
        return 


        
def get_oauth_token():
    # Get DS-API oAuth2 token.
    global oauth_endpoint,oauth_method,client_id,client_secret,oauth_grant_type,oauth_scope,oauth_username,oauth_password,debug_level
    try:
        # Fetch token from token URL
        token=None
        token_url = oauth_endpoint+oauth_method

        if oauth_grant_type=="" or oauth_grant_type=='client_credentials':
            user_creds =  [('client_id', client_id),
                            ('client_secret', client_secret), 
                            ('grant_type', 'client_credentials')]      
        else:
             user_creds =  [('client_id', client_id),
                            ('client_secret', client_secret), 
                            ('grant_type', oauth_grant_type),
                            ('scope',oauth_scope),
                            ('username',oauth_username),
                            ('password',oauth_password)]   

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response_token = requests.post(url=token_url, data=user_creds, verify=False)

        if response_token.status_code!=200: 
           raise Exception("HTTPError: "+str(response_token.status_code)+" getting token")

        token_dict = response_token.json()
        token = token_dict["access_token"]
        if not token:
            if debug_level==9:
              print("Debug - oAuth Failed with credential:",str(user_creds))
            raise Exception("Error: oAuth create token failed. No token returned.")

    except Exception as exception_msg:
        if debug_level==9:
          print("Debug - oAuth Failed with credential:",str(user_creds))
        raise Exception("Error: oAuth create failed in get_token - "+str(exception_msg))
    finally:
        if debug_level>0:
           print("Debug - Token created is:", str(token))
        return token

def get_ds_api_batch(pii_df,bundles,api_options,api_batch_limit):
    # prepare batches of post requests for DS-API and execute.
    global global_oauth_token
    
    recs_in_batch=0
    recs_posted=0
    body=[]
    try:
      for index,rec in pii_df.iterrows():
        #create querystring from row
        rec_dict=rec.to_dict()
        del rec_dict['id']
        params=(urllib.parse.urlencode(rec_dict, doseq=True)+api_options+"&bundle="+bundles).replace('=None&','=&').replace('=nan&','=&')
        
        body.append(api_method+'?'+params)
        recs_in_batch+=1
        #execute in batches and serialise response
        if recs_in_batch==api_batch_limit:
           responses=execute_dsapi(body)
           serialise_results(pii_df,responses,bundles,recs_posted)
           recs_posted=recs_posted+api_batch_limit
           recs_in_batch=0
           body=[]
      # execute the remainder and serialise response  
      if recs_in_batch>0:
        responses=execute_dsapi(body)
        serialise_results(pii_df,responses,bundles,recs_posted)
    except Exception as e:
        raise Exception("Exception in DS-API batch creation - "+str(e))
    
def execute_dsapi(body):
    # Execute the batch of requests
    global api_endpoint,api_method,dsapi_tenantid,dsapi_role,debug_level

    batch_endpoint = api_endpoint+'/batch/'+api_method.split('/')[2]
    if dsapi_tenantid and dsapi_role:
        batch_endpoint=batch_endpoint+("?role="+dsapi_role+"&tenant="+dsapi_tenantid).replace('=None&','=&').replace('=nan&','=&')

    headers = {"Accept": "application/json"}
    headers.update({"Authorization": "Bearer {token}".format(token=global_oauth_token)})

    try:
        print("--- Executing DS_API on ",str(len(body))," records")
        start_time = time.time()
        responses_obj=requests.post(url=batch_endpoint, json=body, headers=headers, verify=False)
        if responses_obj.status_code == 200:
            responses=responses_obj.json()
        else:   
            raise Exception("DS-API exception. Return Code=",str(responses_obj.status_code))
    except Exception as e:
        responses=[]
        raise Exception("Exception executing DS-API - "+str(e))
    finally:
        print("--- Total DSAPI execution time on ",str(len(body))," records: {:.3f} seconds ---".format((time.time() - start_time)))
        if debug_level>0:
          print("Debug - batch_endpoint: "+str(batch_endpoint))
          print("Debug - headers:        "+str(headers))
        if debug_level>1: 
          print("Debug - body:           "+str(body))
          print("=======================================")
          print("Debug - responses:      "+str(responses))
          print("=======================================")
        return responses

def serialise_results(pii_df,responses,bundles,recs_index):
    # traverse the json and create key/value pairs for output to a table
    global debug_level
    try:
        start_time = time.time()
        if debug_level>0:
          print("Debug - Serialising DS_API next ",len(responses),". Recs so far ",recs_index)
        post_batch_results(pii_df,responses,bundles,recs_index)
    except Exception as e:
        if debug_level>1:
            print("Debug - Error processing DS-API response:",str(responses))
        raise Exception("Exception serialising DS-API response - "+str(e))
    finally:
        print("--- Total DSAPI serialise time: {:.3f} seconds ---".format((time.time() - start_time)))

def nested_dict_iter(key_parent,nested): 
    # recursive traverse of the response json
    for key, value in nested.items():
        if isinstance(value, abc.Mapping):
            yield from  nested_dict_iter(key_parent+"."+key,value)
        elif type(value) is list:
            if type(value[0]) is dict:
                for i in value[0]:
                    yield key_parent+"."+key+"."+str(i), value[0][i]
            else: 
                yield key_parent+"."+key, value[0]

        else: 
            yield key_parent+"."+key, value

def post_batch_results(pii_df,responses,bundles,start_index):
    index=start_index
    try:
        for response in responses:
            post_result(pii_df.at[index,'id'],bundles,nested_dict_iter('dsapi',response))
            index+=1
    except Exception as e:
        raise Exception("Exception in DS-API reponse in post_batch_results - "+str(e))

def post_result(customer_id,bundles,keyvalue_enhance):
    global global_db_client,dest_table,global_dest_dict
    try:
        for enhanced in keyvalue_enhance: 
          record = pd.Series([ customer_id, str(enhanced[0]), str(enhanced[1]).replace("'",""), str(bundles) ], index=global_dest_df.columns)
          global_dest_dict.append(record)
    except Exception as e:
       raise Exception("Exception posting to output dict in post_result - ",str(e))     
       
def create_dest_table():
  global dest_table,global_db_client
  try:
    global_db_client.query('CREATE TABLE IF NOT EXISTS '+dest_table+' (  time bigint, customer_id bigint, key varchar,value varchar,  bundle varchar)')
  except Exception as e:
     print("Exception creating dest table: "+dest_table," - ",e)       
     
def bundle_append(bundles='',engine_name='presto', max_recs_to_process=100, api_batch_limit=100):
  # initialise connections
  global global_oauth_token,global_db_client,database_name,global_dest_dict,global_dest_df,api_options
  
  global_oauth_token=get_oauth_token()
  global_db_client=pytd.Client(database=database_name)
  global_dest_df=pd.DataFrame(columns=['customer_id','key','value','bundle'])

  start_time = time.time()

  pii_df=read_source_pii(engine_name, max_recs_to_process)
  
  if isinstance(pii_df, pd.DataFrame):
    if not pii_df.empty:
        get_ds_api_batch(pii_df,bundles,api_options,api_batch_limit)
        if len(global_dest_dict)>0:
          create_dest_table()
          if debug_level>0: print("Debug - Appending dictionary to Dataframe") 
          global_dest_df=global_dest_df.append(global_dest_dict, ignore_index=True)
          if debug_level>0: print("Debug - Loading table to database")
          global_db_client.load_table_from_dataframe(global_dest_df, dest_table, if_exists='append')
    else:
        print("--- No records retrieved for processing through DSAPI ")        
  else:      
    print("--- SQL Extraction error. No records to process through DSAPI ")

  print("--- Total Append execution time: {:.3f} seconds ---".format((time.time() - start_time)))

### Example execution when running outside of Treasure Data. Comment out when running on TD ###
# bundle_append(bundles='id,personIds,householdId,businessIds,ukDemoAttributes,ukPostcode,ukBasicDemographics',max_recs_to_process=1)
# bundle_append(bundles='clientIdentityGraph,id,personIds,householdId,businessIds,ukDemoAttributes',max_recs_to_process=1000,api_batch_limit=1000)
# bundle_append(bundles='inputGlobalAddress',max_recs_to_process=10,api_batch_limit=1000)
# read_results(100000,bundles_filter='clientIdentityGraph.householdId')
# read_results(100000,bundles_filter='')
### End of section ###       