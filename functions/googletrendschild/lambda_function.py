# # Importing libraries ..
# import json
# from pytrends.request import TrendReq
# import pandas as pd
# import time
# import boto3
# import io
# import os
# import logging
# import datetime
# from datetime import datetime

# ######logger#####
# # logLevel = logging.INFOlogging.basicConfig(format="[%(asctime)s] %(levelname)s %(name)s:%(lineno)s - %(message)s", level=logging.INFO)
# logging.getLogger("ParentFucntion")
# logger = logging.getLogger(f"ParentFucntion")

# ######GlobalVariables#######
# global dynamodb,table,ggl_web_col,ggl_img_col,yt_col,bucket,merge_on,SEARCH_TYPE

# dynamodb = boto3.resource('dynamodb')

# ggl_web_col = ['week_start_date', 'google_trends', 'Category']
# ggl_img_col = ['week_start_date', 'google_trends_web', 'Category']
# yt_col = ['week_start_date', 'youtube', 'Category']

# bucket = os.environ['bucket']
# file_path = os.environ['file_path']
# dynamo_table = os.environ['dynamo_table']
# table = dynamodb.Table(dynamo_table)

# merge_on = ["week_start_date","Category"] 

# SEARCH_TYPE='images'


# def google_search_web(term):
#     pytrends = TrendReq()
#     try:
#         logger.info("To get interest over time in google web.")
#         pytrends.build_payload(kw_list=[term], timeframe='today 5-y')
#         ggl_web = pytrends.interest_over_time().iloc[:,0:1].reset_index()
#         ggl_web['Category'] = term        
#         ggl_web.columns = ggl_web_col       
#         ggl_web = ggl_web[ggl_web_col]
#         ggl_web['google_trends'] = pd.to_numeric(ggl_web['google_trends'])
#         return ggl_web    
#     except Exception as e:
#         logger.error(e)
        
        
# def google_serch_img(term):
#     pytrends_im = TrendReq()
#     try:
#         logger.info("To get interest over time in google image.")
#         pytrends_im.build_payload(kw_list=[term], gprop=SEARCH_TYPE, timeframe='today 5-y')
#         ggl_img= pytrends_im.interest_over_time().iloc[:,0:1].reset_index()
#         ggl_img['Category'] = term        
#         ggl_img.columns = ggl_img_col        
#         ggl_img = ggl_img[ggl_img_col]
#         ggl_img['google_trends_web'] = pd.to_numeric(ggl_img['google_trends_web'])
#         return ggl_img    
#     except Exception as e:
#         logger.error(e)
        
        
# def youtube_search_overtime(term):
#     pytrends_yt = TrendReq()
#     try:
#         logger.info("To get interest over time on youtube.")
#         pytrends_yt.build_payload(kw_list=[term], gprop='youtube', timeframe='today 5-y')
#         yt_df = pytrends_yt.interest_over_time().iloc[:,0:1].reset_index()
#         yt_df['Category'] = term        
#         yt_df.columns = yt_col        
#         yt_df = yt_df[yt_col]
#         yt_df['youtube'] = pd.to_numeric(yt_df['youtube'])
#         return yt_df    
#     except Exception as e:
#         logger.error(e)
    
        
        
# def lambda_handler(event, context):
#     try:
#         max_datedata = []
#         categorydata = []
#         terms=event['ProductName1']
        
#         logger.info("Writing the dataframe in a csv file.")
#         for index, i in enumerate(terms):
#             final_ggl_web = google_search_web(i)
#             final_google_serch_img = google_serch_img(i)
#             final_youtube_search_overtime = youtube_search_overtime(i)
#             inital_merge = pd.merge(final_ggl_web, final_google_serch_img, on = merge_on)
#             finaldf = pd.merge(inital_merge,final_youtube_search_overtime, on = merge_on)
#             csv_buffer=io.StringIO()
#             finaldf.to_csv(csv_buffer)
#             content = csv_buffer.getvalue()
#             s3 = boto3.client('s3')
#             folder = f"{file_path}/{i}.csv"
#             s3.put_object(Bucket=bucket, Body=content,Key=folder)
            
#             maxdate = finaldf.groupby(['Category'])['week_start_date'].transform(max)
#             categorydata.append(i)
#             max_datedata.append(maxdate)
            
#         zipped_list = zip(categorydata,max_datedata)
#         dynamozip = list(zipped_list)
#         return dynamozip
    
#     except Exception as e:
#         logger.error(e)
        
        
