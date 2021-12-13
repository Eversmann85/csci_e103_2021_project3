# Databricks notebook source
# MAGIC %md
# MAGIC ## Create company dictionary and the functions

# COMMAND ----------

# create dictionary including company tinker, name and industry sector
com_dict ={}
com_dict['BAC'] =["\"Bank of America\"",'Banks']
com_dict['WFC'] =["\"Wells Fargo\"",'Banks']
com_dict['PNC'] =["\"PNC Financial\"",'Banks']
com_dict['JPM'] =['JP Morgan Chase','Banks']
com_dict['C'] =['Citigroup','Banks']
com_dict['HON'] =['Honeywell','Manufacturing']
com_dict['BA'] =['Boeing','Manufacturing']
com_dict['CAT'] =['Caterpillar','Manufacturing']
com_dict['DE'] =['deere co','Manufacturing']
com_dict['MMM'] =["\"3M co\"",'Manufacturing']
com_dict['ENPH'] =["\"Enphase Energy\"",'Clean Energy']
com_dict['PLUG'] =["\"Plug Power\"",'Clean Energy']
com_dict['VWDRY'] =["\"Vestas Wind Systems\"",'Clean Energy']
com_dict['SEDG'] =["\"Solaredge Technologies\"",'Clean Energy']
com_dict['FSLR'] =["\"First Solar\"",'Clean Energy']
com_dict['COP'] =['ConocoPhillips','Oil & Extraction']
com_dict['EOG'] =["\"EOG Resources Inc\"",'Oil & Extraction']
com_dict['PXD'] =["\"Pioneer Natural Resources\"",'Oil & Extraction']
com_dict['MPC'] =["\"Marathon Petroleum\"",'Oil & Extraction']
com_dict['DVN'] =["\"Devon Energy\"",'Oil & Extraction']
com_dict['IVV'] =["\"iShares Core S P 500 ETF\"",'All']
com_dict['KBWB'] =["\"Invesco KBW Bank ETF\"",'Banks']
com_dict['ICLN'] =["\"iShares Global Clean Energy\"",'Clean Energy']
com_dict['XLI'] =["\"SPDR Select Sector Fund\"",'Manufacturing']
com_dict['IEO'] =["\"iShares U.S. Oil\"",'Oil & Extraction']

# COMMAND ----------

for key in com_dict:
  print(key)

# COMMAND ----------

import pandas as pd
import yfinance as yf
for key in com_dict:
  print(yf.Ticker(key).info.get('longBusinessSummary'))
#   info = yf.Ticker(key).info
#   Ticker.append(info.get('symbol'))
#   CompanyName.append(info.get('shortName'))
#   CompanyDescription.append(info.get('longBusinessSummary'))

# COMMAND ----------

# MAGIC %conda install -c conda-forge pyjanitor

# COMMAND ----------

# define functions  
import requests
import json
import dateutil
import pandas as pd
import time
import datetime
import re
import pyspark.sql.functions as F
from pyspark.sql.types import StringType,TimestampType, DoubleType, LongType, DateType
import numpy as np
import pandas as pd
import janitor


bearer_token ='AAAAAAAAAAAAAAAAAAAAAL7JWAEAAAAApH6GTRKNMfdW9tGyrkgTu3WB%2BTU%3D6UxG7T2oAqlhA2DcphJGBPT20M501jfhtgWcKsor9NtqOn6ZH8'

# function to create headers from bearer_token
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

# function to create url from keyword, return url and the parameters
def create_url(keyword, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
#                     'start_time': start_date,
#                     'end_time': end_date,
                    'max_results': max_results,
#                   'expansions': None,
                    'tweet.fields': 'author_id,created_at,id,source,text',
#                     'user.fields': 'name',
                    'next_token': {}}
    return (search_url, query_params)
# function to return tweets as Json file  
def connect_to_endpoint(url, headers, params, next_token = None,start_date = None):
    params['next_token'] = next_token   #params object received from create_url function
    params['start_time'] =start_date  #date to start request data from
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# functions to create dataframe using Json output
def twitter_to_df(json_resp):
  res =[]
  for i, tweet in enumerate(json_resp['data']):
    user = tweet['author_id']
    date =dateutil.parser.parse(tweet['created_at'])
    id = tweet['id']
#     source = tweet['source']
    text = tweet['text']
    res.append([user,date,id,text])
  resdf =pd.DataFrame(res, columns =['user','date','id','text'])
  return resdf

headers = create_headers(bearer_token)

# put all together, input keyword and start_date, loop through all pages and save result into dataframe, return dataframe and total tweet count
def twitter_for_company(keyword, start_date=None):
  count = 0 # Counting tweets per time period
  total_tweets = 0
#   max_count = 100 # Max tweets per time period
  flag = True
  next_token = None
  comdf =pd.DataFrame(columns=['user','date','id','text', 'key_flag', 'company', 'IndustrySector'])    
  # Check if flag is true
  while flag:
#       Check if max_count reached
#       if count >= max_count:
#           break
#       print("-------------------")
      print("Token: ", next_token)
      company =com_dict[keyword][0]
      url = create_url(company, max_results=100)
      json_response = connect_to_endpoint(url[0], headers, url[1], next_token, start_date)
      result_count = json_response['meta']['result_count']

      if 'next_token' in json_response['meta']:
          # Save the token to use for next call
          next_token = json_response['meta']['next_token']
          print("Next Token: ", next_token)
          if result_count is not None and result_count > 0: # and next_token is not None:
              tdf =twitter_to_df(json_response)
              tdf['key_flag'] = keyword
              tdf['company'] = company
              tdf['IndustrySector'] =com_dict[keyword][1]
              comdf =pd.concat([comdf, tdf])
  #             print("Start Date: ", start_list[i])
  #             append_to_csv(json_response, "data.csv")
              count += result_count
              total_tweets += result_count
  #             print("Total # of Tweets added: ", total_tweets)
  #             print("-------------------")
              time.sleep(1)                
      # If no next token exists
      else:
          if result_count is not None and result_count > 0:
  #             print("-------------------")
  #             print("Start Date: ", start_list[i])
              tdf =twitter_to_df(json_response)
              tdf['key_flag'] = keyword
              tdf['company'] = company
              tdf['IndustrySector'] =com_dict[keyword][1]
              comdf =pd.concat([comdf, tdf])
  #             append_to_csv(json_response, "data.csv")
              count += result_count
              total_tweets += result_count
  #             print("Total # of Tweets added: ", total_tweets)
  #             print("-------------------")
              time.sleep(1)

          #Since this is the final request, turn flag to false to move to the next time period.
          flag = False
          next_token = None
      time.sleep(1)
  return (comdf, count, total_tweets)

def run_twitter_load(com_dict):
  # loop through each company in the dictionary to get all the tweets within 7 days and save in dataframe 
  comdf_all =pd.DataFrame(columns=['user','date','id','text', 'key_flag', 'company', 'IndustrySector'])
  com_meta =[]
  for i in range(len(com_dict)):
    keyword =list(com_dict)[i]
    comdf, count, total_twts=twitter_for_company(keyword)
    comdf_all =pd.concat([comdf_all, comdf])
    com_meta.append([i, keyword, count, total_twts])
    
  return comdf_all, com_meta

def run_twitter_load_daily(com_dict, start_date):
  # loop through each company in the dictionary to get all the tweets starting from the start_date and save in dataframe 
  comdf_all =pd.DataFrame(columns=['user','date','id','text', 'key_flag', 'company', 'IndustrySector'])
  com_meta =[]
  for i in range(len(com_dict)):
    keyword =list(com_dict)[i]
    comdf, count, total_twts=twitter_for_company(keyword, start_date)
    comdf_all =pd.concat([comdf_all, comdf])
    com_meta.append([i, keyword, count, total_twts])
    
  return comdf_all, com_meta



# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tweet_bronze table and Bulk load the previous 7 days data as the starting point

# COMMAND ----------

# the work flow is to run the run_twitter_load to bulk load twitter for all previous 7 days (limited by the Twitter Dev Account), then save to DataBricks as a table

comdf_all, com_meta =run_twitter_load(com_dict)
# reset index 
comdf_all=comdf_all.reset_index(drop=True)
comdf_all.columns =['user', 'date', 'id', 'text', 'Ticker', 'company', 'CompanySector']
# spark.sql(f'DROP TABLE group_3_final_project.tweet_bronze')


# COMMAND ----------

comdf_all['Load_date']=datetime.date.today()
comdf_all1 =comdf_all.copy()
comdf_all1['id'] =np.nan
comdf_all1.columns =['user', 'Twitter_Created_datetime', 'id', 'text', 'Ticker', 'company', 'CompanySector','load_date']

# COMMAND ----------

# spark.sql(f'drop table group_3_final_project.tweet_bronze1')

# COMMAND ----------


# comdf_all.columns =['user', 'date', 'id', 'text', 'Ticker', 'company', 'CompanySector']
print(com_meta)
spark.sql(f"use group_3_final_project")
# create dataframe in spark 
com_sdf=(spark.createDataFrame(comdf_all1)
       .withColumn('Tweet_date',F.to_date(F.col('Twitter_Created_datetime'), 'yyyyMMdd'))
       .where(~F.col('text').contains('Bank of America Stadium')))
com_sdf.write.format('delta').option("inferSchema", 'true').mode('overwrite').partitionBy('Tweet_date').saveAsTable('group_3_final_project.tweet_bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC ## New data is appended to Tweet_bronze table

# COMMAND ----------

start_date =spark.sql(f'select max(Twitter_Created_datetime) from group_3_final_project.tweet_bronze').collect()[0]['max(Twitter_Created_datetime)'].isoformat()+'Z'
start_date

# COMMAND ----------

# for each new subsequent run (daily), get the max(date) from the table and use this as the start_date, then run run_twitter_load_daily to get the new twitter starting from start_date, append the new data to the table (incremental load)
start_date =spark.sql(f'select max(Twitter_Created_datetime) from group_3_final_project.tweet_bronze').collect()[0]['max(Twitter_Created_datetime)'].isoformat()+'Z'
print(start_date)
comdf_all_daily, meta =run_twitter_load_daily(com_dict, start_date)
comdf_all_daily=comdf_all_daily.reset_index(drop=True)
comdf_all_daily['Load_date']=datetime.date.today()
comdf_all_daily['id'] =np.nan
comdf_all_daily.columns  =['user', 'Twitter_Created_datetime', 'id', 'text', 'Ticker', 'company', 'CompanySector','load_date']

spark.sql(f"use group_3_final_project")
# create dataframe in spark 
com_sdf=(spark.createDataFrame(comdf_all_daily)
       .withColumn('Tweet_date',F.to_date(F.col('Twitter_Created_datetime'), 'yyyyMMdd'))
       .where(~F.col('text').contains('Bank of America Stadium'))
       .where(F.dayofweek('Tweet_date').isin([2,3,4,5,6])))
com_sdf.write.format('delta').option("inferSchema", 'true').mode('append').partitionBy('Tweet_date').saveAsTable('group_3_final_project.tweet_bronze')

# COMMAND ----------

spark.sql(f'drop table group_3_final_project.tweet_silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tweet_pre_silver contains sentiment score, use this to create the Tweet_silver table

# COMMAND ----------

tweet_pre_silver =(spark.sql(f'select * from group_3_final_project.tweet_pre_silver')
                          .where(F.dayofweek('Tweet_date').isin([2,3,4,5,6])))
# tweet_pre_silver_df=spark.createDataFrame(tweet_pre_silver_df)
tweet_pre_silver_df = (tweet_pre_silver   #.withColumn("user",F.col("user").cast(StringType())) 
  .withColumn("Twitter_Created_datetime",F.col("Twitter_Created_datetime").cast(TimestampType())) 
  .withColumn("id",F.col("id").cast(StringType())) 
  .withColumn("text",F.col("text").cast(StringType())) 
  .withColumn("Ticker",F.col("Ticker").cast(StringType())) 
  .withColumn("company",F.col("company").cast(StringType())) 
  .withColumn("companySector",F.col("companySector").cast(StringType())) 
  .withColumn('Tweet_date',F.to_date(F.col('Twitter_Created_datetime'), 'yyyyMMdd').cast(DateType()))
  .withColumn('load_date',F.col('load_date').cast(DateType()))
  .withColumn('sentID', F.col('sentID').cast(DoubleType()))
  .drop(F.col('user'))
        )

tweet_pre_silver_df.write.format('delta').option("inferSchema", 'true').mode('overwrite').partitionBy('Tweet_date').saveAsTable('group_3_final_project.tweet_silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine tweet data and financial data to generate the gold table

# COMMAND ----------

# obtain financial data and convert to Pandas dataframe
financial_df =spark.sql(f'select * from group_3_final_project.data_gold').toPandas()

# obtain tweet_silver data 
tweet_pre_silver =spark.sql(f'select * from group_3_final_project.tweet_silver').toPandas()
# tweet_pre_silver =tweet_pre_silver_df.toPandas()


# this part is to convert the real tiwtter date time to the same date time value that financial data used, this need conditional_join fucntion from pyjanitor package
# first create the datetime dataframe 
datetimedf =pd.DataFrame(pd.date_range('2021-12-02 01:30', '2021-12-30',freq ='1h'), columns=['startdatetime'])
datetimedf['enddatetime'] = datetimedf.startdatetime+ pd.Timedelta(hours=1)

# join tiwtter data with datetime dataframe with the (between two time ) join condition 
tweet_pre_agg =tweet_pre_silver.conditional_join(datetimedf,('Twitter_Created_datetime', 'startdatetime', '>='), ('Twitter_Created_datetime', 'enddatetime', '<='))
# get the mean sentiment score for each Ticker and enddatetime
tweet_agg =pd.DataFrame(tweet_pre_agg.groupby(['Ticker', 'companySector','enddatetime']).sentID.mean()).reset_index()
tweet_agg.columns =['Ticker','companySector','enddatetime','sentimentScore']

# merge back to financial data on 'ticker, date' columns, retain all financial data
project_gold_df = pd.merge(financial_df, tweet_agg, left_on =['Date','Ticker','CompanySector'], right_on =['enddatetime','Ticker','companySector'], how= 'left')

project_gold =project_gold_df.drop(columns=['companySector', 'enddatetime', 'index'], axis =1).sort_values(by=['Ticker','Date']).reset_index(drop=True)
project_gold_sdf=(spark.createDataFrame(project_gold))
project_gold_sdf.write.format('delta').option("inferSchema", 'true').mode('overwrite').saveAsTable('group_3_final_project.project_gold')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from group_3_final_project.project_gold where sentimentScore is not null 
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming

# COMMAND ----------


start_date =spark.sql(f'select max(Twitter_Created_datetime) from group_3_final_project.tweet_bronze').collect()[0]['max(Twitter_Created_datetime)'].isoformat()+'Z'
print(start_date)
comdf_all_daily, meta =run_twitter_load_daily(com_dict, start_date)
comdf_all_daily=comdf_all_daily.reset_index(drop=True)
comdf_all_daily['Load_date']=datetime.date.today()
comdf_all_daily.columns =['user', 'Twitter_Created_datetime', 'id', 'text', 'Ticker', 'company', 'CompanySector','load_date']

df =spark.createDataFrame(comdf_all_daily)
df = (df.withColumn("user",F.col("user").cast(StringType())) 
  .withColumn("Twitter_Created_datetime",F.col("Twitter_Created_datetime").cast(TimestampType())) 
  .withColumn("id",F.col("id").cast(StringType())) 
  .withColumn("text",F.col("text").cast(StringType())) 
  .withColumn("Ticker",F.col("Ticker").cast(StringType())) 
  .withColumn("company",F.col("company").cast(StringType())) 
  .withColumn("companySector",F.col("companySector").cast(StringType())) 
  .withColumn('Tweet_date',F.to_date(F.col('Twitter_Created_datetime'), 'yyyyMMdd').cast(DateType()))
  .withColumn('load_date',F.col('load_date').cast(DateType()))
        )
df.printSchema()


df.write.format("delta").option("inferSchema","false").mode("overwrite").saveAsTable("group_3_final_project.tweet_bronze1") 

# twitter_stream = spark.readStream.format("delta").table("group_3_final_project.tweet_bronze1")
# twitter_stream.writeStream.format("delta").trigger(once=True).option("checkpointLocation", f"/tmp/checkpoint-bronze-stream").outputMode("append").table("group_3_final_project.tweet_bronze")

# COMMAND ----------

df =spark.createDataFrame(comdf_all_daily)
df = (df.withColumn("user",F.col("user").cast(StringType())) 
  .withColumn("Twitter_Created_datetime",F.col("Twitter_Created_datetime").cast(TimestampType())) 
  .withColumn("id",F.col("id").cast(StringType())) 
  .withColumn("text",F.col("text").cast(StringType())) 
  .withColumn("Ticker",F.col("Ticker").cast(StringType())) 
  .withColumn("company",F.col("company").cast(StringType())) 
  .withColumn("companySector",F.col("companySector").cast(StringType())) 
  .withColumn('Tweet_date',F.to_date(F.col('Twitter_Created_datetime'), 'yyyyMMdd').cast(DateType()))
  .withColumn('load_date',F.col('load_date').cast(DateType()))
        )
df.printSchema()


df.write.format("delta").option("inferSchema","false").mode("overwrite").saveAsTable("group_3_final_project.tweet_bronze1") 

twitter_stream = spark.readStream.format("delta").table("group_3_final_project.tweet_bronze1")
twitter_stream.writeStream.format("delta").trigger(once=True).option("checkpointLocation", f"/tmp/checkpoint-bronze10-stream").outputMode("append").table("group_3_final_project.tweet_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## To demonstrate the update /insert concept in this project

# COMMAND ----------

# MAGIC %md
# MAGIC #### I manually create some dummy data

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace TEMPORARY VIEW src_df
# MAGIC as
# MAGIC select * 
# MAGIC from group_3_final_project.project_gold where sentimentScore is  null and Ticker ='CAT' and Date <='2021-12-04T14:30:00.000+0000'

# COMMAND ----------

# modify the data by substrating 24 hrs from the Date columns

src_table = (spark.sql(f'select * from src_df')
           .withColumn('Date', F.col('Date')-F.expr('INTERVAL 24 HOURS')))
src_table.createOrReplaceTempView('src_table')

# COMMAND ----------

# MAGIC %md
# MAGIC #### No need to preserve any history data, so Project_gold is type 1 table

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into group_3_final_project.project_gold tgt
# MAGIC using src_table as src
# MAGIC on tgt.Date  =src.Date and tgt.Ticker =src.Ticker 
# MAGIC when matched and (tgt.Adj_close <>src.Adj_Close or tgt.Close <>src.Close or tgt.High <> src.High or tgt.Low <> src.Low or tgt.Open <> src.Open or tgt.Volume <> src.Volume or tgt.sentimentScore <> src.sentimentScore)
# MAGIC then update set tgt.Adj_close =src.Adj_Close,  tgt.Close =src.Close,  tgt.High = src.High,  tgt.Low = src.Low , tgt.Open = src.Open , tgt.Volume = src.Volume, tgt.sentimentScore = src.sentimentScore
# MAGIC when not matched 
# MAGIC then insert(Date, Ticker, Adj_Close,Close,High,Low,Open,Volume,CompanySector,sentimentScore)
# MAGIC      values(Date, Ticker, Adj_Close,Close,High,Low,Open,Volume,CompanySector,sentimentScore)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY group_3_final_project.project_gold

# COMMAND ----------


