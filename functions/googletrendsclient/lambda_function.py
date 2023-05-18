# -*- coding: utf-8 -*-
"""
Short Desc: This programe is `Google Trends` Ingestion and Transformation 
AWS Lambda function for kearney sensing solution

This programe get the data from google api 
using pytrends lib. and store the fetched data in S3 bucket in CSV format
(raw and transformed both)

Usage: This script meant for AWS Lambda function

Event Json: lambda will receive `tenant_id` via event json (args).
Environment Variables: `dynamodb` table name, `env` (dev|uat|prod|test ) , `krney_bucket` (gereric bucket name of keraney) 
IAM Policy: with each new client onboarded IAM policy will be updated for newly generated bucket (S3,KMS) via TF / Platform 

"""

__author__ = "Divesh Chandolia, Suyash Verma"
__copyright__ = "Copyright 2023, Kearney Sensing Solution"
__version__ = "1.0.2"
__maintainer__ = "Divesh Chandolia, Suyash Verma"
__email__ = "dchand01@atkearney.com,"
__date__ = "April 2023"


# builtin imports 
import logging
import os
from io import StringIO
import sys
from functools import reduce
import json
from datetime import datetime, date
import time

# Lib
import pandas as pd
import boto3
from pytrends.request import TrendReq

from boto3.dynamodb.types import TypeDeserializer
deserializer = TypeDeserializer()


# Creating the DynamoDB Client and S3 resource
dynamodb_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
s3_resource = boto3.resource('s3')

# google property list to get trends
gprop_list = ['','youtube','images']

BUCKET = ''
TABLE_NAME = ''

# Data layers in the S3 bucket
RAW_DIR = 'raw-data'
CLEANED_DIR = 'cleaned-data'
TRANSFORMED_DIR = 'transformed-data'
# EXT_DATA_DIR = 'external-data'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def save_csv(df, file_path):
    "Save the DataFrame as CSV in S3 Path"
    try:
        # dst_path = f"{EXT_DATA_DIR}/{file_path}" # client bucket
        # if "0000000000" in BUCKET: # krny generic bucket
        dst_path = file_path
        logger.info(f"Saving file {dst_path}")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)
        s3_resource.Object(BUCKET, dst_path).put(Body=csv_buffer.getvalue())
    except Exception as err:
        logger.error(f"Error while saving: {err}")

def google_trends(terms,GPROP,timeframe='today 3-m'):
    "This function use pytrends lib for fetch google trends data"

    max_terms = 5 # max capacity for pytrends 
    # list of list with max 5 ele in a sublist
    term_set_5 = [terms[i * max_terms:(i + 1) * max_terms] for i in range((len(terms) + max_terms - 1) // max_terms )]
    
    dfs = []
    for terms in term_set_5[:]:
        logger.info(f"terms--{terms}")
        pytrends = TrendReq(retries=5, backoff_factor=5)
        pytrends.build_payload(kw_list=terms, gprop=GPROP, timeframe=timeframe)
        ggl_search = pytrends.interest_over_time()
        ggl_search = ggl_search[terms]
        dfs.append(ggl_search)
    
    df = reduce(lambda left, right: pd.merge(left, right, on=['date'], how='outer'), dfs)
    df.reset_index(inplace=True)
    df = pd.melt(df, id_vars =['date'], value_vars =df.columns)
    if not GPROP:GPROP='web'
    df.rename(columns={'variable':'category','value':GPROP},inplace=True)
    return df

def get_trends(terms,last_run):
    
    # terms = terms_details['details']['terms']
    # last_run = terms_details['details']['date']

    last_run = datetime.strptime(f"{last_run}", "%Y-%m-%d").date()
    today = date.today()
    if today <= last_run:
        return pd.DataFrame()

    timeframe = f"{str(last_run)} {str(today)}"
    logger.info(f"timeframe: {timeframe}")
 
    all_dfs = []
    for gprop in gprop_list:
        logger.info(f"{gprop}")
        ggl_res = google_trends(terms,gprop,timeframe=timeframe)
        all_dfs.append(ggl_res)
        
    final_df = reduce(lambda left, right: pd.merge(left, right, on=['date','category'], how='outer'), all_dfs)
    return final_df



def get_terms_details(tenant_id):
    "This function returns configuration like search terms, last date, newly added terms"

    # code to get terms from dynamoDB
    # Use the DynamoDB client get item method to get a single item
    logger.info(f"{tenant_id},{TABLE_NAME}")
    response = dynamodb_client.get_item(
        TableName=TABLE_NAME,
        Key={
            'tenant_id': {'S': tenant_id},
        }
    )

    res = response['Item']
    deserialized_res = {k: deserializer.deserialize(v) for k, v in res.items()}
    return deserialized_res

def update_db(tenant_id,max_date_str,new_terms_lst = None):
    "It updated the dynamodb table"

    logger.info("Updating dynamodb table")
    logger.info(f"{tenant_id} {max_date_str}")

    table = dynamodb_resource.Table(TABLE_NAME)
    response = table.get_item(Key={'tenant_id': tenant_id})
    item = response['Item']
    item['details']['date'] = max_date_str
    if new_terms_lst:
        item['details']['terms'].extend(new_terms_lst)
        item['details']['new_terms']['terms'] = []
    table.put_item(Item=item)

    # dynamodb_client.update_item(
    #     TableName=TABLE_NAME,
    #     Key={ 'tenant_id': {'S': tenant_id}, }
    #     AttributeUpdates={
    #         'date': max_date_str,
    #     },
    # )
    
def validate(date_text):
    try:
        date.fromisoformat(date_text)
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

def lambda_handler(event, context):

    global BUCKET
    global TABLE_NAME
    
    NEW_TERMS_AVAILABLE = False
    
    logger.info("-- Google Trends Data Collection Starts --")
    
    # get tenant_id from event    
    tenant_id = event.get('tenant_id')  # or '2117584738'
    # get dynamoDB table name from env
    TABLE_NAME = os.environ.get('dynamodb') # or 'dev-krny-google-trends-client'
    ENV = os.environ.get('env') # or 'dev'
    BUCKET = f"krny-spi-{tenant_id}-{ENV}"

    if tenant_id == "0000000000":
        BUCKET = os.environ.get('krny_bucket')
    
    # get details from table
    terms_details = get_terms_details(tenant_id)
    logger.debug(terms_details)
    terms = terms_details['details']['terms']
    last_run = terms_details['details']['date']
    
    terms_list = [{'date':last_run,'terms':terms}]
    
    validate(last_run)
    
    try:
        new_terms = terms_details['details']['new_terms']['terms']
        new_terms_date = terms_details['details']['new_terms']['date']
        if new_terms and new_terms_date:
            validate(new_terms_date)
            logger.info("New terms")
            terms_list.append({'date':new_terms_date,'terms':new_terms})
            NEW_TERMS_AVAILABLE = True
        else:
            logger.info("No New terms")
    except Exception as err:
        logger.error(f"Exception while accesssing new terms: {err}")
         
    for term_dict in terms_list:
        trends = get_trends(term_dict['terms'],term_dict['date'])
        if not trends.empty:

            cur_timestamp = str(int(time.time()))
            cur_date = str(date.today())
            # Save raw data
            file_path = f"{RAW_DIR}/google_trends/data/{cur_date}/{cur_timestamp}.csv"
            save_csv(trends,file_path)

            trends.rename(columns= {
                    'date':'Date'
                },inplace=True)

            # get max date to update in DB
            max_date = trends['Date'].max()
            max_date_str = max_date.strftime("%Y-%m-%d")

            # Save CSV in bucket as raw data
            file_path = f"{CLEANED_DIR}/google_trends/data/{cur_date}/{cur_timestamp}.csv"
            save_csv(trends,file_path)

            trends.rename(columns= {
                'web':'Google_Trend_Interest_over_time_web',
                'images':'Google_Trend_Interest_over_time_image',
                'youtube':'Google_Trend_Interest_over_time_youtube'
            },inplace=True)
            trends['month_year'] = pd.to_datetime(trends['Date']).dt.to_period('M')
            trends['month_year'] = trends['month_year'].astype(str)
            # drop date col
            trends.drop('Date', axis=1, inplace=True)

            df_monthly=trends.groupby(['category','month_year']).sum().reset_index().rename(columns={'index':'Date'})
            # Save in transformed-data 
            file_path = f"{TRANSFORMED_DIR}/google_trends/data/{cur_date}/{cur_timestamp}.csv"
            save_csv(df_monthly,file_path)

            # Update in DB
            if NEW_TERMS_AVAILABLE:
                update_db(tenant_id,max_date_str,terms_list[1].get('terms'))
            else:
                update_db(tenant_id,max_date_str)
            
        else:
            logger.info("No new data to fetch")
            return {
                'statusCode': 200,
                'body': json.dumps('No new data')
            }
        
    return {
            'statusCode': 200,
            'body': json.dumps('Updated')
        }
    
