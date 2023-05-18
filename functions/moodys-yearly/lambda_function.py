#moodys new yearly
import requests
import hashlib
import hmac
import boto3
from io import StringIO
from datetime import datetime, timezone, timedelta
import json
import sys
import logging
import os
import numpy
import pandas as pd
from functools import reduce



#opening sessions
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
glue = boto3.client('glue')
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager'
)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)



def lambda_handler(event, context):
    
    
    def get_key(secret_name):
        
        "fetching the api keys from secret manager"
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
                )
            secret = get_secret_value_response['SecretString']
            sec = json.loads(secret)
            return sec
        
        except Exception as err:
            logger.error(f"Error while fetching keys: {err}")
            raise Exception(f"while fetching keys: {err}")
    
    
    def get_data(dynamodb_table):
    
        "This function returns all the mnemonics and date from the table"
    
        try:
            table = dynamodb.Table(dynamodb_table)
            response = table.scan()
            data = response['Items']
            df = pd.DataFrame(data)
            df = df[['mnemonics', f'{date_frequency}']]
            df.columns = ['mnemonics', 'date']
            df = df.groupby('date')['mnemonics'].apply(list)
            return df
        
        except Exception as err:
            logger.error(f"Error while fetching data from table: {err}")
            raise Exception(f"while fetching data from table: {err}")
    
    
    try:
        #getting bucket_name & glue_job_name from environment
        bucket_name = os.environ['bucket']
        gluejobname = os.environ['gluejobname']
        file_path = os.environ['file_path']
        dynamodb_table = os.environ['dynamodb_table']
        file_name = os.environ['file_name']
        secret_name = os.environ['secret_name']
        date_frequency = os.environ['date_column']
        freq_code = os.environ['freq_code']
        mapping_file_path = os.environ['mapping_file_path']
        mapping_file_name = os.environ['mapping_file_name']
    except Exception as err:
        logger.error(f"Error while reading environmental variables : {err}")
    
    sec = get_key(secret_name)
    accKey = sec['accKey']
    encKey = sec['encKey']
    
    mnemonic_df = get_data(dynamodb_table)
    nmemonic_desc_dict = {}
    
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d")
    date_time =str(date_time)
    dfs = []
    dfs_final = []
    for date, mnemonic_list in mnemonic_df.items():
        startDate = date
        startDate = datetime.strptime(startDate, "%Y-%m-%d")
        startDate = startDate + timedelta(days=1)
        startDate = startDate.strftime("%Y-%m-%d")
        nm_list = mnemonic_list
        max_nm = 25
        nm_lists_25 = [nm_list[i * max_nm:(i + 1) * max_nm] for i in range((len(nm_list) + max_nm - 1) // max_nm )]
        for nm_list_25 in nm_lists_25[:]:
            nmstr = ";".join(nm_list_25)
            try:
                timeStamp = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
                payload = bytes(accKey + timeStamp, "utf-8")
                signature = hmac.new(bytes(encKey, "utf-8"), payload, digestmod=hashlib.sha256)
                head = {"AccessKeyId":accKey, "Signature":signature.hexdigest(), "TimeStamp":timeStamp}
                url_str = f"https://api.economy.com/data/v1/multi-series?m={nmstr}&startDate={startDate}&endDate={date_time}&freq={freq_code}"
                response = requests.get(url_str,headers=head)
                response = response.json()
                response = response['data']
            except Exception as err:
                logger.error(f"Error while getting data from api  : {err}")
        
            try:
                dfs_25 = []
                for res in response:
                    nmemonic_desc_dict[res['mnemonic']] = res['description']
                    for x in range(0,len(res['data'])):
                        res['data'][x]['value']=str(res['data'][x]['value'])
                    df = pd.DataFrame(res['data'])
                    df = df.rename(columns={'date':'Date','value':res['mnemonic']})
                    df = df.iloc[:-1, :]
                    dfs_25.append(df)
            except Exception as err:
                logger.error(f"Error while converting response to datframe : {err}")
            
            
            try:
                df_25 = reduce(lambda left, right: pd.merge(left, right, on=['Date'], how='outer'), dfs_25)
                dfs.append(df_25)
            except Exception as err:
                logger.error(f"Error while merging 25 mnemonics dataframe  : {err}")
    
        try:
            final_df = reduce(lambda left, right: pd.merge(left, right, on=['Date'], how='outer'), dfs)
            dfs_final.append(final_df)
        except Exception as err:
            logger.error(f"Error while merging all dataframe into one dataframe : {err}")
    
    
    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['Date'], how='outer'), dfs_final)
    
    
    if not df_merged.empty:
        try:
            max_date = df_merged.Date.max()
            str_max_date = str(max_date)
            table = dynamodb.Table(dynamodb_table)
            response = table.scan()
            data = response['Items']
            for item in data:
                mnemonic = item['mnemonics']
                response = table.get_item(Key={'mnemonics': mnemonic})
                item = response['Item']
                item[f'{date_frequency}'] = str_max_date
                table.put_item(Item=item)
        except Exception as err:
            logger.error(f"Error while passing max date to dynamodb  : {err}")
    
    
        try:
            filename =  file_path + date_time + '/' + file_name
            csv_buffer = StringIO()
            df_merged.to_csv(csv_buffer,index=False)
            s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer.getvalue())
        except Exception as err:
            logger.error(f"Error while saving mnemonics data in s3  : {err}")
    
        try:
            #saving the file having mnemonics and its description
            nemonic_df = pd.DataFrame.from_dict(nmemonic_desc_dict,orient='index').reset_index()
            nemonic_df = nemonic_df.rename(columns={'index':'mnemonic',0:'description'})
            filename1 =   mapping_file_path + mapping_file_name
            csv_buffer1 = StringIO()
            nemonic_df.to_csv(csv_buffer1,index=False)
            s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename1, Body=csv_buffer1.getvalue())
        except Exception as err:
            logger.error(f"Error while saving mnemonics mapping file  : {err}")
        
    
        try:
            #Glue Job Invoke
            runId = glue.start_job_run(JobName=gluejobname)
            status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
            print("Job Status : ", status['JobRun']['JobRunState'])
        except Exception as err:
            logger.error(f"Error while starting glue job : {err}")
    
    else:
        logger.info("There is no new data to ingest")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
        
    }
    
    
    
            
            
            
    
    
   
    
    
  

