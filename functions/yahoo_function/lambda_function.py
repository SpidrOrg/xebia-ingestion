#Importing libraries
import boto3
import json
import yahoo_fin.stock_info as si
import pandas as pd
import logging
import sys
import io
import os
import urllib
from io import StringIO
from datetime import datetime, timedelta

#opening sessions

glue = boto3.client('glue')
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def lambda_handler(event, context):
    
    #getting bucket_name & glue_job_name from environment
    try:
        table_name = os.environ['dynamodb_table']
        bucket_name = os.environ['bucket']
        gluejobname = os.environ['gluejobname']
        file_folder = os.environ['file_path']
        file_name = os.environ['file_name']
    except Exception as err:
        logger.error(f"Error while reading environmental variables: {err}")
        raise Exception(f"Error while reading environmental variables : {err}")
    
    try:
        table = dynamodb.Table(table_name)
    except Exception as err:
        logger.error(f"Error while reading dynamodb table : {err}")
        raise Exception(f"Error while reading dynamodb table : {err}")
    
    yahoo_df = pd.DataFrame()
    
    #Storing response of Dynamodb in Data
    response = table.scan()
    data = response['Items']
    
    #iteration in the dynamodDb for ticker
    
    for item in data:
        
        #Fetching ticker & date from DynamoDb
        try:
            ticker = item['ticker']
            index = item['index']
            date = item['date']
        except Exception as err:
            logger.error(f"Error while fetching data from dynamodb table : {err}")
            sys.exit(0)
        
        #Fetching data for each ticker
        try:
            df = si.get_data(ticker, start_date = date, interval='1mo', index_as_date=False)
        except Exception as err:
            logger.error(f"Error while fetching data from api : {err}")
        
        try:
            df.rename(columns = {'ticker':'colname', 'date':'Date'}, inplace = True)
            df.drop(index=df.index[-1],axis=0,inplace=True)
            mask = df['Date'] > date
            df = df.loc[mask]
        except Exception as err:
            logger.error(f"Error while renaming columns : {err}")
        
        
        #Getting the maximum date to dynamodb
        
        if not df.empty:
            
            try:
                max_date = df.Date.max()
                str_max_date = str(max_date)
                response = table.get_item(Key={'index': index})
                item = response['Item']
                item['date'] = str_max_date
                table.put_item(Item=item)
                yahoo_df = pd.concat([yahoo_df, df], ignore_index=True, axis=0)
            except Exception as err:
                logger.error(f"Error while getting max date : {err}")
    
    #Parsing it into s3 bucket
    
    if not yahoo_df.empty:
        try:
            now = datetime.now()
            date_time = now.strftime("%Y-%m-%d")
            filename = file_folder + str(date_time) + '/' + file_name
            csv_buffer = StringIO()
            yahoo_df.to_csv(csv_buffer,index=False)
            s3.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer.getvalue())
        except Exception as err:
            logger.error(f"Error while converting to csv : {err}")
    
          #glue job invoke
        try:
            runId = glue.start_job_run(JobName=gluejobname)
            status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        except Exception as err:
            logger.error(f"Error while starting glue job : {err}")
    else:
        logger.info("There is no new data to ingest")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
        
    }
