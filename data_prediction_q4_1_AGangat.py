#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
COCT DS Code Challenge Question 4.1

This script is the code submsission for Ameera Gangat for the challenge question as seen below 

Please use sr_hex.csv dataset, only looking at requests from the Water and Sanitation Services department.

Please chose two of the following:

4.1 Time series challenge: Predict the weekly number of expected service requests per hex for the next 4 weeks.
"""
#%% Import libraries
import pandas as pd
import timeit
import os
import os.path
from os import path
import requests
import warnings
from statsmodels.tsa.ar_model import AutoReg
import numpy
warnings.simplefilter(action='ignore', category=FutureWarning)

#%% Functions to use
# create a difference transform of the dataset
def difference(dataset):
	diff = list()
	for i in range(1, len(dataset)):
		value = dataset[i] - dataset[i - 1]
		diff.append(value)
	return numpy.array(diff)
 
# Make a prediction give regression coefficients and lag obs
def predict(coef, history):
	yhat = coef[0]
	for i in range(1, len(coef)):
		yhat += coef[i] * history[-i]
	return yhat
 

# predict the future values based on past series data
def predict_count(series):
    """
    An autoregression code function that takes in timeseries data and calculates a prediction based on the data
    """
    X = series.values

    train, test = X[1:len(X)-4], X[len(X)-4:]
    # train autoregression
    window = 1
    model = AutoReg(train, lags=2)
    model_fit = model.fit()
    coef = model_fit.params
    history = [train[i] for i in range(len(train))]
    predictions = list()
    for t in range(len(test)):
    	yhat = predict(coef, history)
    	obs = test[t]
    	predictions.append(yhat)
    	history.append(obs)
    return predictions

def file_appender(input_data,output_path):
    with open(output_path,'a') as f:
        f.writelines(input_data)
        
def mp_print(*args):
    all_args=args
    op_str = ''
    tables = ''
    for arg in args:
        if type(arg)!=dict:
            op_str+=arg
        elif type(arg)==dict:
            print("blah")
    
    op_str = "<p >"+op_str+" "+"</p>"+"\n"
    op_str+=tables
    file_appender(op_str,output_html_path)

#%% Download the required data files
print("Downlaoding the data files")
starttime = timeit.default_timer()
sr_hex_file_url = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz"
sr_hex_gz_fname = sr_hex_file_url.split("/")[-1]

if path.exists("sr_hex.csv.gz"):
    print("service request hex file exists, do not downlaod file")
else:
    r = requests.get(sr_hex_file_url,stream=True)
    with open(sr_hex_gz_fname, "wb") as f:
        r = requests.get(sr_hex_file_url)
        f.write(r.content)

#%%Extract the data and save to dataframes and variables
# extract sr.csv and sr_hex.csv
print("extract sr_hex.csv and saving to dataframes and variables")
sr_hex_df = pd.read_csv(sr_hex_gz_fname, compression='zip', header=0, sep=',', quotechar='"')
#filter data to keep sr from water and sanitation department only
sr_hex_df = sr_hex_df[sr_hex_df['department'] == 'Water and Sanitation']
# filter data to exclude service requests with no location data
sr_hex_df = sr_hex_df[sr_hex_df['h3_level8_index'] != '0']

# sort data by h3_level_index
rslt_df = sr_hex_df.sort_values(["h3_level8_index"],inplace = False)
rslt_df = rslt_df.reset_index(drop=True)

#%% calculate each service request week number from start date based on CreationDate

rslt_df["CreationDate"] = pd.to_datetime(rslt_df["CreationDate"])

earliest_date = min(rslt_df["CreationDate"])

rslt_df["week_num"] = (rslt_df["CreationDate"] - earliest_date)/7
rslt_df['week_num'] = rslt_df['week_num'].dt.days.astype('int16')

#%%
# sort data by h3_level8_index and CreationDate
rslt2_df = rslt_df.sort_values(["h3_level8_index","CreationDate"],inplace = False)
#rslt2_df = rslt_df.sort_values(["h3_level8_index","CreationDate","week_num"],inplace = False)
rslt2_df = rslt2_df.reset_index(drop=True)

# find all the unique h3_level8_index values from the sr file
h3_list = rslt2_df['h3_level8_index'].unique()
h3_dict = {}
for i in range(len(h3_list)):
    h3_dict.setdefault(h3_list[i],[])

h3_predict_dict = {}
for i in range(len(h3_list)):
    h3_predict_dict.setdefault(h3_list[i],[])

h3_predict_4weeks_dict = {}
for i in range(len(h3_list)):
    h3_predict_4weeks_dict.setdefault(h3_list[i],[])

#%% using the calculated week number from for each sr and hex number, use the predict function to predict future number of requests for each hex number    
for i in range(len(h3_list)):   
    week_num_counts = rslt2_df.loc[rslt2_df["h3_level8_index"]==h3_list[i]]["week_num"].value_counts().sort_index()
    h3_dict[h3_list[i]].append(week_num_counts)
    if week_num_counts.shape[0]<=10:
            h3_predict_dict[h3_list[i]].append(1)
    else:
            if i%1000==0:
                print("now at hex number {}".format(i))
            h3_predict_dict[h3_list[i]].append(predict_count(week_num_counts))

#%% generate html report
output_html_path = "data_prediction_q4_1_AGangat.html"

if path.exists("data_prediction_q4_1_AGangat.html"):
    os.remove("data_prediction_q4_1_AGangat.html")

for key, value in h3_predict_dict.items():
    mp_print("location hex number {} is predicted to get {} service requests respectively over the next 4 weeks".format(key,value[:5]))

print("The time taken to complete this script is  {:.2f} minutes".format((timeit.default_timer() - starttime)/60))
