#!/usr/bin/env python3
import csv

import time
import datetime
from calendar import timegm
import os
import sys
import re

from dateutil.parser import parse
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from pprint import pprint, pformat

import urllib3
urllib3.disable_warnings()
from urllib.parse import urlencode

plan_cache = {}
entry_id = 1
int_err_cnt  = 0
int_host_cnt =0
from dateutil.parser import parse


def add_file_entries(modified_entry,entry_id,id,filename,checksum,actual__checksum,expected__checksum,source__proto__host,dest__proto__host):
     modified_entry["_id"] = entry_id
     modified_entry["origin_id"] = id
     modified_entry['filename'] = filename
     modified_entry['checksum_failure'] = checksum
     modified_entry["actual_checksum"] = actual__checksum
     modified_entry["expected_checksum"] = expected__checksum  
     modified_entry['source_proto_host'] = source__proto__host
     modified_entry['dest_proto_host'] = dest__proto__host
     

'''process the stdout text to extract filenames and their cheksum failure status and
generate featureset for processed event

rtype: [transfer_files,execution_files],hostname
All the files transferred in the task may be captured in stderr_text and stdout_text which can be parsed to extract this information
Also, execution hostname can be read from these files

Parameters:
file : contains the content of stdout/stderr_text from event in ElasticSearch
hostname: hostname for the event 
'''

def process_stdout(file,hostname):
    all_transfers = []
    integrity_err_files = []
    execution__hostname = str(hostname)
     
    if file=="":
       
        return [all_transfers,integrity_err_files],execution__hostname
    try:
        file = file.replace("%0A"," ")
        if "Executing on host" in file:
            host = re.compile('Executing on host ([^ ]+) .*')
            execution__hostname = host.findall(file)[0]
        transfer_file = file.split("  INFO:  ")     
        for info_log in transfer_file:
            if  "Copying" in info_log:
             
              
                file_transferred = info_log.split()[3]
                     		
                file_transferred = file_transferred.split("//")
               
                if file_transferred[0] !="file:":
                    source_proto_host = file_transferred[0]+"//"+file_transferred[1].split("/")[0]
                else:
                    source_proto_host = execution__hostname
                dest_info = info_log.split()[5]
            
                if dest_info.split("/")[0]!="file:":
                    dest_info  = dest_info.split("//")
                    dest_proto_host = dest_info[0]+"//"+dest_info[1].split("/")[0]
                else:
                    dest_proto_host = execution__hostname
                transfer_dict = {"filename":file_transferred[1].split("/")[-1],"source_proto_host":source_proto_host,"dest_proto_host":dest_proto_host}  
                all_transfers.append(transfer_dict)
               
            if "ERROR" in info_log:
                error_log = info_log.split("    ERROR:  ")[1]
        
        search_str = "##################### Checking file integrity for input files #####################"
        if search_str not in file:
            return [all_transfers,[]],""
        integrity_ind_begin = file.index(search_str)
        integrity_str = file[integrity_ind_begin+len(search_str):]
        
        while "Integrity check: " in integrity_str:
           
            integrity_err = integrity_str.split("Integrity check:")[1]
            integrity_err_str = integrity_err.split(":")
           
            expected = re.compile('Expected checksum \((.*)\) does')
            actual = re.compile('calculated checksum \((.*)\)')
            integrity_err_files.append({"filename":integrity_err_str[0].strip(),"actual_checksum":actual.findall(integrity_err_str[1])[0],
                                        "expected_checksum":expected.findall(integrity_err_str[1])[0],"source_proto_host":""
					,"dest_proto_host":""})
            integrity_str = integrity_str[20:]
        for transfer_file in all_transfers:
            for integrity_file in integrity_err_files:
                      if integrity_file["filename"] == transfer_file["filename"]: 
                         integrity_file["source_proto_host"] = transfer_file["source_proto_host"]
                         integrity_file["dest_proto_host"] =  transfer_file["dest_proto_host"]
     

        integrity_files = [f["filename"] for f in integrity_err_files]
        all_transfers_ = []
        all_transfers_ = [x for x in all_transfers if x not in integrity_files] 
       
        return [all_transfers_,integrity_err_files],execution__hostname
    except:
         
        return [all_transfers,integrity_err_files],execution__hostname

'''
process the incoming events
and return a list of processed events
Parameters
     client:  ElasticSearch client
     start_dt : start time for query processing
     end_dt: end times for query processing
     index: index of where to store the new events
'''
def get_processed_events(client, start_dt,end_time,index):
     
    start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
    print('Quering for data starting with ' + start_time)
    
    s = Search(using=client, index='pegasus-composite-events-*') \
               .query('match', event='stampede.job_inst.composite') \
	       .query('match', pegasus__version = '4.9.2panorama', _expand__to_dot = False)\
               .filter('range', ** {'@timestamp': {'gt': start_time, 'lt':end_time}}) 

    s = s.sort('ts')  
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
        # modified entry is the dictionary for the event generated after processing the event from ElasticSearch 
        modified_entry={}
        id = entry['_id']       
        entry = entry['_source']
        
#        if entry['user']!='bamboo':  # only consider bamboo users, if not remove the condition
#           continue
       
        if entry['int_error_count']!=0:  # counting total integrity errors
           global int_err_cnt
           # counting integrity errors in the session
           int_err_cnt += entry['int_error_count']

        # create the processed event with new values
        modified_entry['root_xwf_id'] = entry['xwf__id']
        datetime = entry['@timestamp'].split(".")
        modified_entry['_index'] = index
        modified_entry['start_time'] = entry['ts']
        modified_entry['end_time'] = entry['local__dur'] if "local__dur" in entry else 10
        modified_entry['end_time'] = modified_entry['start_time']+modified_entry['end_time']
        modified_entry['job_id'] = entry['job__id']
        modified_entry['submit_hostname'] = entry['submit__hostname'] if "submit__hostname" in entry else entry["submit_hostname"]
        modified_entry['@timestamp'] = entry['@timestamp']
        modified_entry['execution_hostname'] = entry['hostname'] if "hostname" in entry else ""
        modified_entry['source_id'] = id
        modified_entry['execution_site'] = entry['site']
        modified_entry['job_type'] = entry['jobtype'] if "jobtype" in entry else ""
        modified_entry['job_exitcode'] = entry['exitcode']
        modified_entry['retry_attempt'] = 1
        modified_entry['executable'] = 1
        modified_entry['user_submit'] = entry['wf_user']
        modified_entry['user_remote']= entry['user']
        modified_entry['local_dur'] = entry['local__dur'] if "local__dur" in entry else 0
       	modified_entry['_type'] = 'pegasus-composite-events-'

       	# processing stderr_text and stdout_text to get transfer files and their checksum failure status
        if 'stderr__text' in entry: 
            stderr_processed,execution__hostname_1 = process_stdout(entry['stderr__text'],modified_entry['execution_hostname'])
                 
            transfer_files_stderr = stderr_processed[0]
            integrity_err_files_stderr = stderr_processed[1]
            if execution__hostname_1 != "":
               modified_entry['execution_hostname'] = execution__hostname_1
        else:
            transfer_files_stderr,integrity_err_files_stderr = [],[]
        if 'stdout__text' in entry:	    
            stdout_processed,execution__hostname_2  = process_stdout(entry['stdout__text'],modified_entry['execution_hostname'])
            transfer_files_stdout = stdout_processed[0]
            integrity_err_files_stdout = stdout_processed[1] 
            if execution__hostname_2 != "":
               modified_entry['execution_hostname'] = execution__hostname_2
        else:
            transfer_files_stdout,integrity_err_files_stdout = [],[]
        
        transfer_files = transfer_files_stderr+transfer_files_stdout
        integrity_err_files = integrity_err_files_stderr+integrity_err_files_stdout
              
        global entry_id 
        for file in transfer_files:
           
            if file==[]:
                continue
            modified_entry = modified_entry.copy()
            add_file_entries(modified_entry,entry_id,id,file["filename"],0,"","",file["source_proto_host"],file["dest_proto_host"])
            entry_id = entry_id + 1
            data.append(modified_entry)
        for file in integrity_err_files:
            if file ==[]:
                continue
            modified_entry = modified_entry.copy()
            add_file_entries(modified_entry,entry_id,id,file["filename"],1,file["actual_checksum"],file["expected_checksum"],file["source_proto_host"],file["dest_proto_host"])
            #modified_entry ["orig_id"]=modified_entry["orig_id"][:-2]+str(ord(modified_entry["orig_id"][-1])+1)
            #modified_entry["_id"] = entry_id
            entry_id = entry_id + 1
            #modified_entry['filename'] = file["filename"]
            #modified_entry["actual__checksum"] = file["actual__checksum"]
            #modified_entry["expected__checksum"] = file["expected__checksum"]
            #modified_entry['checksum__failure'] = 1
            #modified_entry['source__proto__host'] = file["source__proto__host"]
            #modified_entry['dest__proto__host'] = file["dest__proto__host"]
            data.append(modified_entry)
    return data


def main():

    #connecting to ElasticSerch Client 
    client = Elasticsearch('https://galactica.isi.edu/es/', http_auth = ('', ''),timeout=60000,max_retries=10)

    # the number of days data that needs to be collected 
    start_dt = datetime.datetime.utcnow() - datetime.timedelta(days=30)
    end_dt = datetime.datetime.utcnow()
    
    current_dt = start_dt
    time_delta = current_dt+datetime.timedelta(hours=5)
    print(current_dt,end_dt)
    results = True

    #index to write events to
    index = "processed-composite-workflow-events"

    #file containing the processed events
    csvfile = open("events.csv",'w')

    #to delete the existing index
    '''
    if client.indices.exists(index+"*"):
       client.indices.delete(index+'*')
    '''
     
    # output_rows contains a list of processed events returned by the module get_processed_events    
    output_rows = []

    while time_delta < end_dt:
        
        results = False
        events = get_processed_events(client, current_dt,time_delta,index+current_dt.strftime('%Y.%m.%d'))
        
        print(len(events))
        if events!= []:

            #save the event in ElasticSearch with new index 
            result = helpers.bulk(client,events,refresh=True)
            output_rows = output_rows + events 
        
        current_dt = time_delta + datetime.timedelta(seconds = 1)
        time_delta = time_delta + datetime.timedelta(hours = 5)

    # store the eprocessed events in csv file 
    if len(output_rows)>0:    
        dict_writer = csv.DictWriter(csvfile,fieldnames=output_rows[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(output_rows)
    csvfile.close()
    
    print("Total events: ",len(output_rows))
    global int_err_count
    print("Total integrity errors occurred: ", int_err_cnt)

if __name__=="__main__":
    main()
    
