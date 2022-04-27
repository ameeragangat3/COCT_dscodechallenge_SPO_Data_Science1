#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ameera.Gangat
COCT DS Code Challenge Question 2

This script is the code submsission for Ameera Gangat for the challenge question as seen below 

2. Initial Data Transformation (if applying for a Data Engineering and/or Science Position)
Join the file city-hex-polygons-8.geojson to the service request dataset, such that each service request is assigned to a single H3 hexagon. Use the sr_hex.csv file to validate your work.

For any requests where the Latitude and Longitude fields are empty, set the index value to 0.

Include logging that lets the executor know how many of the records failed to join, and include a join error threshold above which the script will error out. Please also log the time taken to perform the operations described, and within reason, try to optimise latency and computational resources used.
"""
#%% Import libraries to be used in this script
import numpy as np
import pandas as pd
import timeit
from os import path
import requests
import zipfile
import json
#%% Functions to use
def haversine_np(lat1,lon1,lat2,lon2):
    """
    This function calculates the distance in km between 2 lat/long coordinate points
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

def dist_2_coords(lat1,lon1,lat2,lon2):
    """
    This function determines the coordinate index which is the closest to the centroid coordinate
    """
    dist = haversine_np(lat1,lon1,lat2,lon2)
    return np.argmin(dist)

#%% Download the required data files
print("Downlaoding the data files")
starttime = timeit.default_timer()
# downlaod the data files if it does not exist
sr_file_url = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr.csv.gz"
sr_hex_file_url = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz"
sr_gz_fname = sr_file_url.split("/")[-1]
sr_hex_gz_fname = sr_hex_file_url.split("/")[-1]

if path.exists("sr.csv"):
    print("service request file exists, do not downlaod file")
else:
    r = requests.get(sr_file_url,stream=True)
    #r = wget.downlaod(sr_file_url)
    with open(sr_gz_fname, "wb") as f:
        r = requests.get(sr_file_url)
        f.write(r.content)
    #with zipfile.ZipFile(sr_gz_fname, 'r') as zip_ref:
        #zip_ref.extractall('.')
        
if path.exists("sr_hex.csv"):
    print("service request hex file exists, do not downlaod file")
else:
    r = requests.get(sr_hex_file_url,stream=True)
    with open(sr_hex_gz_fname, "wb") as f:
        r = requests.get(sr_hex_file_url)
        f.write(r.content)
    #with zipfile.ZipFile(sr_hex_gz_fname, 'r') as zip_ref:
        #zip_ref.extractall('.')

#%% Extract the data and save to dataframes and variables
# extract sr.csv and sr_hex.csv
print("extract sr.csv and sr_hex.csv and saving to dataframes and variables")
sr_df = pd.read_csv(sr_gz_fname, compression='zip', header=0, sep=',', quotechar='"')
sr_hex_df = pd.read_csv(sr_hex_gz_fname, compression='zip', header=0, sep=',', quotechar='"')


h3_level8_index = list(sr_hex_df["h3_level8_index"])

sr_coords = [sr_df["Latitude"],sr_df["Longitude"]]
sr_coords = np.transpose(np.asarray(sr_coords))

# save city-hex-polygons-8.geojson features into city_hex_data variable 
city_hex_fname = 'city-hex-polygons-8.geojson'
with open(city_hex_fname) as f:
    city_hex_data = json.load(f)
city_hex_data = city_hex_data["features"]

#%% Calculate which hex number each lat/long from sr falls into and assign it
print("calculating which hex number each lat/long from sr falls into and assign it (this might take a while)")
cent_coords_all = np.zeros([len(city_hex_data),2])
for i in range(0,len(city_hex_data)):
    cent_coords_all[i,:] = [city_hex_data[i]["properties"]["centroid_lat"],city_hex_data[i]["properties"]["centroid_lon"]]

h3_level8_index_new = []
sr_coords1 = []
validation_list = []
for i in range(0,len(sr_coords[:])):
    sr_coords1 = [sr_coords[i,0],sr_coords[i,1]]
    if (np.isnan(sr_coords1[0]) or np.isnan(sr_coords1[1])):
        h3_level8_index_new.append('0')
    else:
        sr_coords1 = np.asanyarray(sr_coords1)
        sr_coords1 = np.tile(sr_coords1, reps=len(city_hex_data))
        sr_coords1 = np.reshape(sr_coords1,[len(city_hex_data),2])
        found_hex_ind = dist_2_coords(sr_coords1[:,0],sr_coords1[:,1],cent_coords_all[:,0],cent_coords_all[:,1])
        h3_level8_index_new.append(city_hex_data[found_hex_ind]["properties"]["index"])

    #validate
    if h3_level8_index_new[i]==h3_level8_index[i]:
        validation_list.append(1)
    else:
        validation_list.append(0)
        
percentage_validated = np.sum(validation_list) / len(sr_coords[:]) *100
print("city-hex-polygons-8.geojson h3_level8_index joined to sr.csv service request file with a success rate of sr validation of {} %".format(percentage_validated))
print("The time taken to complete this script for data extraction is  {:.2f} minutes".format((timeit.default_timer() - starttime)/60))

















