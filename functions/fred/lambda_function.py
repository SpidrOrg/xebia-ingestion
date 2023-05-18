#FRED new code
import json
import boto3
import base64
from datetime import datetime, timezone
import requests
import os
from io import StringIO
import csv
import pandas as pd
import logging
import sys


#Creating The Empty Data Frame 
df = pd.DataFrame()
glue = boto3.client('glue')


logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def lambda_handler(event, context):
    
    try:
        #Getting data from environment variable
        table_name = os.environ['dynamodb_table']
        bucket_name = os.environ['bucket']
        gluejobname = os.environ['gluejobname']
        folder_path = os.environ['file_path']
        secret_names = os.environ['secret']
    except Exception as err:
        logger.error(f"Error while reading environmental variables: {err}")
        sys.exit(0)
    
    s3 = boto3.client('s3')
    
    
    #Session for Key of Fred In Secret manager
    secret_name = secret_names
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )



    try:
        #Opening Session for dynamodb
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
    except Exception as err:
        logger.error(f"Error while reading dynamodb table : {err}")
        sys.exit(0)
    
    
    try:
        #Getting secret manager
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response['SecretString']
        sec = json.loads(secret)
    except Exception as err:
        logger.error(f"Error while fetching secret key value : {err}")
        sys.exit(0)
    
    
    
    
    try:
        #Storing secret key 
        key = sec['key']
    except Exception as err:
        logger.error(f"Error while fetching data from secret manager table : {err}")
        sys.exit(0)
    
    
    #Storing response of Dynamodb in Data
    response = table.scan()
    data = response['Items'] 
    
    
    
    
    
    # #iteration in the dynamodDb for Serial_Id
    for item in data:
        
        #Fetching Serial_ID & bucket name from DynamoDb
        api_1 = item['api'] 
        series_Id = item['Series_ID']
        index = item['index']
        date = item['Latest_Date']
        
        
        try:
            #Hitting api
            api=f"{api_1}?series_id={series_Id}&api_key={key}&file_type=json&observation_start={date}&output_type=2"
        except Exception as err:
            logger.error(f"Error while fetching data from api : {err}")
            sys.exit(0)
        
        try:
            #Parsing the Data into Json
            resp = requests.get(api)
            resp = resp.json()
            resp = resp['observations']
            df = pd.DataFrame(resp)
        except Exception as err:
            logger.error(f"Error while making df from api : {err}")
            sys.exit(0)
        
        
        try:
            #Renaming Logic
            dict = {
                'date': 'DATE',
            }
            for i in range(1,len(df.columns)):
                dict[df.columns[i]]=df.columns[i].split('_')[0]
            df.rename(columns=dict,inplace=True)
            mask = df['DATE'] > date
            df = df.loc[mask]
        except Exception as err:
            logger.error(f"Error while renaming the df : {err}")
            
        
        if not df.empty:
            try:
                #Getting the maximum date to dynamodb
                max_date = df.DATE.max()
                response = table.get_item(Key={'index': index})
                item = response['Item']
                item['Latest_Date'] = max_date
                table.put_item(Item=item)
            except Exception as err:
                logger.error(f"Error while updating dynamodDb : {err}")
                sys.exit(0)
                
                
           
            #Parsing it into s3 bucket
            now = datetime.now()
            date_time = now.strftime("%Y-%m-%d")
            filename = folder_path + str(date_time) + '/' + series_Id + '.csv'
            csv_buffer = StringIO()
            df.to_csv(csv_buffer,index=False) 
            try:
                s3.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer.getvalue())
            except Exception as err:
                logger.error(f"Error while saving : {err}")
        else:
            logger.info(f"There is no new data to ingest for Series_ID:{series_Id}")
        
        
        
        
    try:
        # Glue Job 
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as err:
        logger.error(f"Error while starting glue job : {err}")
    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
        
    }
