#new code for covid updated
import json
import os
import pandas as pd
import io
import logging
import sys
from io import StringIO
import boto3
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from datetime import datetime
from dateutil import parser

#opening sessions
s3 = boto3.client("s3")
glue = boto3.client("glue")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def lambda_handler(event, context):
    
    
    #getting bucket_name & glue_job_name from environment
    try:
        bucket_name = os.environ['bucket']
        gluejobname = os.environ['gluejobname']
        prefix_covid = os.environ['prefix']
        url = os.environ['url']
        base_url = os.environ['base_url']
        covidcases_filename = os.environ['covidcases_filename']
        vaccinedata_filename = os.environ['vaccine_filename']
    except Exception as err:
        logger.error(f"Error while reading environmental variables : {err}")
        raise Exception(f"Error while reading environmental variables : {err}")

    
    #getting latest date folder to decide incremental or base pull
    folders = []
    
    try:
        result = s3.list_objects(Bucket=bucket_name, Prefix=prefix_covid, Delimiter='/')
    except Exception as err:
        logger.error(f"Error while reading max folder from s3 bucket : {err}")
        sys.exit(0)
    
    
    for prefix in result.get('CommonPrefixes', list()):
        p1 = prefix.get('Prefix', '')
        folders.append(p1)
    if not folders:
        startdate = datetime.now() + relativedelta(years=-3)
        statdate = startdate.strftime("%Y-%m-%d")
        
    else:
        maxdate_folder = max(folders)
        startdate = parser.parse(maxdate_folder, fuzzy=True)
        startdate = startdate.strftime("%Y-%m-%d")
    
    #For vaccine file
    
    #passing the url
    url = url
    
    try:
        vaccine_df = pd.read_csv(url, parse_dates = ['Date'])
    except Exception as err:
        logger.error(f"Error while getting data from url : {err}")
    
    
    #Locating the data from latest date in dataframe
    
    try:
        mask = vaccine_df['Date'] > startdate
        df = vaccine_df.loc[mask]
    except Exception as err:
        logger.error(f"Error while locating latest date : {err}")
    
    if not df.empty:
        
    #Parsing data into s3 bucket
        max_date = df.Date.max()
        max_date = max_date.strftime("%Y-%m-%d")
        str_max_date = str(max_date)
        now = datetime.now()
        date_time = now.strftime("%Y-%m-%d")
        filename = prefix_covid + max_date + '/' + vaccinedata_filename
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)
        
        try:
            s3.put_object(Bucket =bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer.getvalue())
        except Exception as err:
            logger.error(f"Error while saving : {err}")
            
    
    
    #for covidcases
    
    
    #createdateslist for pulling the data for each folder
    
    try:
        enddate = datetime.now()
        enddate = enddate.strftime("%Y-%m-%d")
        dateslist = pd.date_range(startdate,enddate,freq='d')
        dateslist = [x.date() for x in dateslist]
        dateslist = [datetime.strftime(x, "%m-%d-%Y") for x in dateslist]
    except Exception as err:
        logger.error(f"Error while making date list : {err}")
    
    #url link
    base_url = base_url

    
    #data pulling for each date in dateslist
    cases_df=pd.DataFrame()
    for datestring in dateslist:
        url = base_url + datestring + ".csv"
        try:
            tempdf = pd.read_csv(url)
            tempdf['Date']=datetime.strptime(datestring, "%m-%d-%Y")
            cases_df = pd.concat([cases_df, tempdf], ignore_index=True, axis=0)
        except:
            continue
    
    mask1 = cases_df['Date'] > startdate
    cases_df = cases_df.loc[mask1]        
    
    #parsing the data into s3
    if not cases_df.empty:
        filename1 = prefix_covid + max_date + '/' + covidcases_filename
        csv_buffer1 = StringIO()
        cases_df.to_csv(csv_buffer1,index=False)
        
        try:
            s3.put_object(Bucket =bucket_name, ContentType='text/csv', Key=filename1, Body=csv_buffer1.getvalue())
        except Exception as err:
            logger.error(f"Error while saving : {err}")
        
        try:
            runId = glue.start_job_run(JobName=gluejobname)
            status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
            print("Job Status : ", status['JobRun']['JobRunState'])
        except Exception as err:
            logger.error(f"Error while starting glue job : {err}")
            
        return {
            'statusCode': 200,
            'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
            
        }
    else:
        logger.info(f"There is no new data to ingest")
    
    
   
    
    return {
        'statusCode': 200,
        'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
        
    }
