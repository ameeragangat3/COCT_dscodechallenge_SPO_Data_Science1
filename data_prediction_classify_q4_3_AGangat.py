#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
COCT DS Code Challenge Question 4.3

This script is the code submsission for Ameera Gangat for the challenge question as seen below: 

Please use sr_hex.csv dataset, only looking at requests from the Water and Sanitation Services department.

4.3 Classification challenge: Classify a hex as formal, informal or rural based on the data derived from the service request data.
Feel free to use any other data you can find in the public domain, except for task (3).

"""
#%% Import libraries
import pandas as pd
import timeit
import os
import os.path
from os import path
import requests
import zipfile
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

#%% Functions to use
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
    #with zipfile.ZipFile(sr_hex_gz_fname, 'r') as zip_ref:
        #zip_ref.extractall('.')

#%%Extract the data and save to dataframes and variables
# extract sr.csv and sr_hex.csv
print("extract sr_hex.csv and saving to dataframes and variables")
sr_hex_df = pd.read_csv(sr_hex_gz_fname, compression='zip', header=0, sep=',', quotechar='"')
#filter data to keep sr from water and sanitation department only
sr_hex_df = sr_hex_df[sr_hex_df['department'] == 'Water and Sanitation']
# filter data to exclude service requests with no location data
sr_hex_df = sr_hex_df[sr_hex_df['h3_level8_index'] != '0']
#sr_hex_df = sr_hex_df[0:10000]

# sort data by h3_level_index
rslt_df = sr_hex_df.sort_values(["h3_level8_index"],inplace = False)
rslt_df = rslt_df.reset_index(drop=True)

# find all the unique h3_level8_index values from the sr file
h3_list = rslt_df['h3_level8_index'].unique()
h3_dict = {}
for i in range(len(h3_list)):
    h3_dict.setdefault(h3_list[i],[])

#%% Classify each hex to informal, rural or formal based on sr data    
"""
Classify each hex to informal, rural or formal based on sr data

This is done by the following conditions:
    if for each hex number group there exists a CodeGroup == WATER  - INFORMAL SETTLEMENTS Or SEWER  - INFORMAL SETTLEMENTS AND OfficialSuburbs contains FARM
    then this hex is classified as rural
    
    if the above condition fails but for each hex number group there exists a CodeGroup == WATER  - INFORMAL SETTLEMENTS Or SEWER  - INFORMAL SETTLEMENTS
    then this hex is classified as informal
    
    if both conditions fail then the hex is classified as formal
    
"""    
for i in range(0,len(h3_list)):
    x = rslt_df["h3_level8_index"]==h3_list[i]
    if (((rslt_df["CodeGroup"][x] == 'WATER  - INFORMAL SETTLEMENTS') | (rslt_df['CodeGroup'][x] == 'SEWER  - INFORMAL SETTLEMENTS')) & ((rslt_df["OfficialSuburbs"][x].str.contains("FARM")))).any():
        h3_dict[h3_list[i]].append("rural")

    elif (rslt_df["OfficialSuburbs"][x].str.contains("FARM")).any():
        h3_dict[h3_list[i]].append("rural")


    elif ((rslt_df["CodeGroup"][x] == 'WATER  - INFORMAL SETTLEMENTS') | (rslt_df['CodeGroup'][x] == 'SEWER  - INFORMAL SETTLEMENTS')).any():    
        h3_dict[h3_list[i]].append("informal")
    
    else:
        h3_dict[h3_list[i]].append("formal")

#%% print results to an html file
output_html_path = "data_prediction_classify_q4_3_AGangat.html"
if path.exists("data_prediction_classify_q4_3_AGangat.html"):
    os.remove("data_prediction_classify_q4_3_AGangat.html")

for key, value in h3_dict.items():
    #print("location hex number {} is predicted to get {} service requests respectively over the next 4 weeks".format(key,value[:5]))
    mp_print("location hex number {} is classified to be: ({}), based on the service request data".format(key,value[0]))

print("The classification of each hex location/number can be found in the html report, or contained within this code under dictionary variable h3_dict")
print("The time taken to complete this script for data extraction is  {:.2f} minutes".format((timeit.default_timer() - starttime)/60))
