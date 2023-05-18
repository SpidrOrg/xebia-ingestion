
#Importing Lib.
import json
import pandas as pd
import openpyxl
import boto3
import logging
import io
import os
import sys
from dateutil import parser
from io import StringIO
from datetime import datetime, timedelta
import s3fs
from functools import reduce
import urllib.parse
import pathlib


#opening sessions
s3_resource = boto3.resource('s3')
s3 = boto3.client('s3')
s3_cleint = boto3.client('s3')
# glue = boto3.client('glue')

#Creating Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

BUCKET = ''

#  code specfic constants
conversion = "raw_conversion_dashboard"
totaltraffic = "raw_totaltraffic_sources"
# Data layers in the S3 bucket
RAW_DIR = 'raw-data'
CLEANED_DIR = 'cleaned-data'
TRANSFORMED_DIR = 'transformed-data'

def save_csv(df, file_path):
    "Save the DataFrame as CSV in S3 Path"
    try:
        dst_path = file_path
        logger.info(f"Saving file {dst_path}")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)
        s3_resource.Object(BUCKET, dst_path).put(Body=csv_buffer.getvalue())
    except Exception as err:
        logger.error(f"Error while saving: {err}")

def read_csv(file_path, **kwargs):
    "Read csv data file and return pd dataframe"
    logger.info(f"Reading file: {file_path}")
    try:
        response = s3_cleint.get_object(Bucket=BUCKET, Key=file_path)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            return pd.read_csv(response.get("Body"))
    except Exception as err:
        logger.error(f"Error while reading: {err}")

def lambda_handler(event, context):
    
    global BUCKET

    BUCKET = event['Records'][0]['s3']['bucket']['name']
    bucket = s3_resource.Bucket(f'{BUCKET}')

    path = event['Records'][0]['s3']['object']['key']
    # get env variables
    conversion_sheet = os.environ.get('conversion_sheet','Direct')
    path_for_csv = os.environ.get('path')

    SRC_DIR1 = os.path.dirname(path)
    logger.info(f"First file:{SRC_DIR1}")

    if conversion in path:
        SRC_DIR2 = SRC_DIR1.replace(conversion,totaltraffic)
        logger.info(f"Second file:{SRC_DIR2}")
    elif totaltraffic in path:
        SRC_DIR2 = SRC_DIR1.replace(totaltraffic,conversion)
        logger.info(f"Second file:{SRC_DIR2}")
    else:
        logger.info(f"Unknown file:{path}")

    file_list = []
    for objects in bucket.objects.filter(Prefix=SRC_DIR1):
        path_str = objects.key
        if path_str.endswith(".xlsx"):
            file_list.append(path_str)

    for objects in bucket.objects.filter(Prefix=SRC_DIR2):
        path_str = objects.key
        if path_str.endswith(".xlsx"):
            file_list.append(path_str)

    logger.info(f"file_list: {file_list}")
    conversion_df = pd.DataFrame()
    traffic_df_monthly = pd.DataFrame()
    if len(file_list) >=2:

        for xlsx_file in file_list:

            xlsx_file_path = f"s3://{BUCKET}/{xlsx_file}"

            base_path = os.path.split(xlsx_file)[0]
            base_dir = pathlib.PurePath(base_path).name
            file_name = pathlib.PurePath(xlsx_file).stem
            raw_csv_file = f"{path_for_csv}{base_dir}/{file_name}.csv"
            logger.info(f"raw_csv_file: {raw_csv_file}")

            if conversion in xlsx_file:
                
                try:
                    conversion_df = pd.read_excel(xlsx_file_path, sheet_name=conversion_sheet)
                    # saving raw csv file
                    save_csv(conversion_df,raw_csv_file)
                except Exception as err:
                    logger.error(f"Error while reading: {xlsx_file_path}; {err}")

            elif totaltraffic in xlsx_file:
                try:
                    traffic_df = pd.read_excel(xlsx_file_path, sheet_name=None)
                    # print("traffic_df: ", traffic_df)
                except Exception as err:
                    logger.error(f"Error while reading : {err}")
    
                try:
                    traffic_df_monthly=pd.DataFrame()
                    dfs = [traffic_df[key] for key in traffic_df.keys() if 'Monthly' in key]
                    traffic_df_monthly = pd.concat(dfs)

                    # saving raw csv file
                    save_csv(traffic_df_monthly,raw_csv_file)
                    # conversion_df = read_csv(xlsx_file_csv.replace(""))
                except Exception as err:
                    logger.error(f"Error while preprocessing: {err}")
    
            else:
                logger.error(f"Unknown file: {xlsx_file}")
        print("traffic_df_monthly",traffic_df_monthly.shape)
        print("conversion_df",conversion_df.shape)
        # Transformation
        # if True:
        if (not traffic_df_monthly.empty and not conversion_df.empty ):
            logger.info("Both df ready")
            try:
                df1 = conversion_df
                # Remove Group Average
                df1=df1.loc[df1['Domains']!='Group Average']
                # Drop duplicates, keeping new extract
                df1 = df1.drop_duplicates(
                    ['Domains', 'Time Period'], keep='first')
                # Add Segment to Amazon domain
                df1.loc[df1['Domains'] == 'amazon.com', 'Domains'] = df1.loc[df1['Domains'] == 'amazon.com', 'Domains']\
                    + '-' + df1.loc[df1['Domains'] =='amazon.com', 'Segment'].fillna('')
                
                # # save cleaned data
                # file_path = f""
                # save_csv_cleaned(df1,file_path)

                del df1['Segment']
                df1['Domains'].value_counts()
                df1 = df1.pivot(index='Time Period',columns='Domains').reset_index()
                df1.columns = df1.columns.map('_'.join)
                df1.columns = ['SW_' + x for x in df1.columns]
                df1 = df1.rename(columns={'SW_Time Period_': 'Time Period'})

                # tototraffic
                df2 = traffic_df_monthly
                df2 = df2.drop_duplicates(
                    ['Domain', 'Time Period', 'Channel Traffic'], keep='first')
                # Append both files
                # df2 = df2_new.append(df1, ignore_index=True)
                # Drop duplicates, keeping new extract
                df2 = df2.drop_duplicates(
                    ['Domain', 'Time Period', 'Channel Traffic'], keep='first')
                df2['Domain'].value_counts()

                # Replace string <5000 with constant 5000
                df2.loc[df2['Channel Traffic'] ==
                        '<5,000.00', 'Channel Traffic'] = 2500
                df2['Channel Traffic'] = df2['Channel Traffic'].astype('float')

                # # save cleaned data
                # save_csv_cleaned(df2,file_path)

                # Calculate monthly ol traffic per domain
                df2 = df2.groupby(['Time Period', 'Domain']).agg(
                    {'Channel Traffic': 'sum'}).reset_index()
                # Pivot
                df2 = df2.pivot(index='Time Period',
                                columns='Domain').reset_index()
                df2.columns = df2.columns.map(''.join)
                df2.columns = [x.replace('Channel Traffic', '')
                            for x in df2.columns]

                # Rename columns based on variable tracker
                df2 = df2.rename(columns={'amazon.com': 'SW_amazon_ol_Traffic',
                                        'homedepot.com': 'SW_homedepot_ol_traffic',
                                        'lowes.com': 'SW_Lowes_ol_traffic',
                                        'truevalue.com': 'SW_truevalue_OL_traffic'})
                
                # Merge both datasets
                df_merged = pd.merge(df1, df2, on='Time Period', how='left')
                df_merged= df_merged.rename(columns={'Time Period':'Date'})
                # return df_merged
            
                if not df_merged.empty:
                    folder = path_for_csv.replace(RAW_DIR,TRANSFORMED_DIR)
                    save_csv(df_merged, f"{folder}{base_dir}/similarweb_clean.csv")
            
            except Exception as err:
                logger.error(f"Error while transformation: {err}")
                

