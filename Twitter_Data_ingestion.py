# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

databaseName = f"{databaseName}_Final_project"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")


# COMMAND ----------

# create company tinker and name dictionary
com_dict ={}
# com_dict['BNKS.AX'] ='BETABANKS ETF UNITS'
com_dict['BAC'] ="\"Bank of America\""
com_dict['WFC'] ="\"Wells Fargo\""
com_dict['PNC'] ="\"PNC Financial\""
com_dict['JPM'] ='JP Morgan Chase'
com_dict['C'] ='Citigroup'
# com_dict['XLI'] ='Industrial Select Sector SPDR Fund'
com_dict['HON'] ='Honeywell'
com_dict['BA'] ='Boeing'
com_dict['CAT'] ='Caterpillar'
com_dict['DE'] ='deere co'
com_dict['MMM'] ="\"3M co\""
# com_dict['ICLN'] ='Bank of America Corp'
com_dict['ENPH'] ="\"Enphase Energy\""
com_dict['PLUG'] ="\"Plug Power\""
com_dict['VWDRY'] ="\"Vestas Wind Systems\""
com_dict['SEDG'] ="\"Solaredge Technologies\""
com_dict['FSLR'] ="\"First Solar\""
# com_dict['IEO'] ='US Oil & Gas Exploration & Production Index by iShares'
com_dict['COP'] ='ConocoPhillips'
com_dict['EOG'] ="\"EOG Resources Inc\""
com_dict['PXD'] ="\"Pioneer Natural Resources\""
com_dict['MPC'] ="\"Marathon Petroleum\""
com_dict['DVN'] ="\"Devon Energy\""


# COMMAND ----------

# define functions  
import requests
import json
import dateutil
import pandas as pd
import time
import  pyspark.sql.functions as F
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
                    'max_results': max_results,
#                     'expansions': None,
                    'tweet.fields': 'author_id,created_at,id,source,text',
#                     'user.fields': 'name',
                    'next_token': {}}
    return (search_url, query_params)
# function to return tweets as Json file  
def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
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

# put all together, input keyword, loop through all pages and save result into dataframe, return dataframe and total tweet count
def twitter_for_company(keyword):
  count = 0 # Counting tweets per time period
  total_tweets = 0
#   max_count = 100 # Max tweets per time period
  flag = True
  next_token = None
  comdf =pd.DataFrame(columns=['user','date','id','text', 'key_flag', 'company'])    
  # Check if flag is true
  while flag:
#       Check if max_count reached
#       if count >= max_count:
#           break
#       print("-------------------")
      print("Token: ", next_token)
      company =com_dict[keyword]
      url = create_url(company, 100)
      json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
      result_count = json_response['meta']['result_count']

      if 'next_token' in json_response['meta']:
          # Save the token to use for next call
          next_token = json_response['meta']['next_token']
          print("Next Token: ", next_token)
          if result_count is not None and result_count > 0: # and next_token is not None:
              tdf =twitter_to_df(json_response)
              tdf['key_flag'] = keyword
              tdf['company'] = company
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

# COMMAND ----------

# loop through each company in the dictionary to get all the tweets within 7 days and save in dataframe 
comdf_all =pd.DataFrame(columns=['user','date','id','text', 'key_flag', 'company'])
com_meta =[]
for i in range(len(com_dict)):
  keyword =list(com_dict)[i]
  comdf, count, total_twts=twitter_for_company(keyword)
  comdf_all =pd.concat([comdf_all, comdf])
  com_meta.append([i, keyword, count, total_twts])

# COMMAND ----------

# count number of tweet for each company
comdf_all.groupby('key_flag').key_flag.count()

# COMMAND ----------

# reset index 
comdf_all=comdf_all.reset_index(drop=True)

# COMMAND ----------

comdf_all.tail()

# COMMAND ----------

import re
# spark.sql(f"CREATE DATABASE IF NOT EXISTS group_3_final_project")
spark.sql(f"use group_3_final_project")
# create dataframe in spark 
com_sdf=spark.createDataFrame(comdf_all).withColumn('Tweet_date',F.to_date(F.col('date'), 'yyyyMMdd'))
com_sdf.write.format('delta').option("inferSchema", 'true').mode('overwrite').partitionBy('Tweet_date').saveAsTable('group_3_final_project.tweet_bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC case  when key_flag='BNKS' then 'Banks'
# MAGIC       when key_flag='BAC' then 'Banks'
# MAGIC       when key_flag='WFC' then 'Banks'
# MAGIC       when key_flag='PNC' then 'Banks'
# MAGIC       when key_flag='JPM' then 'Banks'
# MAGIC       when key_flag='C' then 'Banks'
# MAGIC       when key_flag='XLI' then 'Manufacturing'
# MAGIC       when key_flag='HON' then 'Manufacturing'
# MAGIC       when key_flag='BA ' then 'Manufacturing'
# MAGIC       when key_flag='CAT' then 'Manufacturing'
# MAGIC       when key_flag='DE' then 'Manufacturing'
# MAGIC       when key_flag='MMM' then 'Manufacturing'
# MAGIC       when key_flag='ICLN' then 'Clean Energy'
# MAGIC       when key_flag='ENPH' then 'Clean Energy'
# MAGIC       when key_flag='PLUG' then 'Clean Energy'
# MAGIC       when key_flag='VWS' then 'Clean Energy'
# MAGIC       when key_flag='SEDG' then 'Clean Energy'
# MAGIC       when key_flag='FSLR' then 'Clean Energy'
# MAGIC       when key_flag='IEO' then 'Oil & Extraction'
# MAGIC       when key_flag='COP' then 'Oil & Extraction'
# MAGIC       when key_flag='EOG' then 'Oil & Extraction'
# MAGIC       when key_flag='PXD' then 'Oil & Extraction'
# MAGIC       when key_flag='MPC' then 'Oil & Extraction'
# MAGIC       when key_flag='DNV' then 'Oil & Extraction'
# MAGIC       else 'na'
# MAGIC       end as sector
# MAGIC from group_3_final_project.tweet_bronze

# COMMAND ----------



# COMMAND ----------


