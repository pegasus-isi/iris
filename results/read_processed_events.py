#!/usr/bin/env python3
import csv

import time
import datetime
from calendar import timegm
import os
import sys
import re
import pandas as pd
from dateutil.parser import parse
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


import urllib3
urllib3.disable_warnings()
from urllib.parse import urlencode

from dateutil.parser import parse


'''
process the incoming events
and return a list of processed events
Parameters
     client:  ElasticSearch client
     start_dt : start time for query processing
     end_dt: end times for query processing
     index: index of where to store the new events
'''
def read_processed_events(client, start_dt,end_time,index):
     
    start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
    print('Quering for data starting with ' + start_time)
    
    s = Search(using=client, index=index) \
               .filter('range', ** {'@timestamp': {'gt': start_time, 'lt':end_time}}) 

    
    s = s[:s.count()]
    if len(list(s)) == 0:
        return []
    else: 
        print(len(list(s)))
      
    try:
        response = s.execute()
        if not response.success():
            raise
    except Exception as e:
        print(e, 'Error accessing Elasticsearch')
        sys.exit(1)
    data = []
    for entry in response.to_dict()['hits']['hits']:
        data.append(entry['_source'])
       
    return data

def print_columns(df):
    #Counting total integrity errors that occurred in the data
    integrity_err_count = df['checksum__failure'].sum()
    print("Total integrity errors ",integrity_err_count)
    print(df.columns)

def print_top_hosts(df):
    top_integrity_err_exec_host = df.groupby(["execution__hostname"])["checksum__failure"].agg('mean').sort_values(ascending=False)#.filter(lambda x :x["checksum__failure"]!=0)
    #top_integrity_err_exec_host =pd.DataFrame(index=top_integrity_err_exec_host.index,values=top_integrity_err_exec_host)
    print(top_integrity_err_exec_host)
    top_integrity_err_exec_host.plot.bar()
    import matplotlib.pyplot as plt  
    # # plot between 2 attributes 


    plt.xlabel("execution__hostname") 
    plt.ylabel("checksum_failure") 
    plt.legend()
    plt.show()



def main():

    #connecting to ElasticSerch Client 
    client = Elasticsearch('https://galactica.isi.edu/es/', http_auth = ('iris', 'aiw8geeZ'),timeout=60000,max_retries=10)

    # the number of days data that needs to be collected 
    start_dt = datetime.datetime.utcnow() - datetime.timedelta(days=14)
    end_dt = datetime.datetime.utcnow()
    
    current_dt = start_dt
    print(current_dt,end_dt)
    
    #index to read events from
    index = "processed-composite-workflow-events*"
    
    time_delta = current_dt + datetime.timedelta(hours =5)
        
    output_rows = []
    while time_delta < end_dt:
        
        #read data for  intital 5 hours from starting from last 14 days
        events = read_processed_events(client, current_dt,time_delta,index)
        
        if events!= []:
            output_rows = output_rows + events   
        current_dt = time_delta + datetime.timedelta(seconds = 1)
        time_delta = time_delta + datetime.timedelta(hours = 5)


    df = pd.DataFrame.from_dict(output_rows)
    print(df.head())


      
 

if __name__=="__main__":
    main()
    
