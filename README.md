

The script to pull events from ElasticSearch and process them to derive set of attributes required for clustering is present in /extractor directory

**Clone**

```
Clone the repository using https://github.com/pegasus-isi/iris
```

**Execute**

SSH to ISI workflow machine and run the script inside
```
python workflow-composite-event-process.py
```

**Reading events from ElasticSearch**

```
// Connection to client using hostname, authorization, timeout, max_retries. 
 client = Elasticsearch('https://galactica.isi.edu/es/', 
 http_auth = ('<user>', '<password>'),timeout=60000,max_retries=10)
```

```timeout:``` the time in seconds before which connection didn't expire\
```max_retries:``` number of retries before connection expires


***Search query to read the events /logs***
```
s = Search(using=client, index='pegasus-composite-events-*') \
               .query('match', event='stampede.job_inst.composite') \  # query the events matching the index
	             .query('match', pegasus__version = '4.9.2panorama', _expand__to_dot = False)\  # query the events with matching pegasus-version
               .filter('range', ** {'@timestamp': {'gt': start_time, 'lt':end_time}}) # extract events only between intervals: start_time to end_time

s = s.sort('ts')   #sort events based on timestamp
s = s[:s.count()]  #read all the events in the array (s.count() specifies the total count of events)
```

```index:``` index in elastic search from where data needs to be picked\
```match:``` this argument is to look for events matching the argument value

Exeute the search query to get json response and read each entry in ElasticSearch
```
 try:
     response = s.execute()
     if not response.success():
     raise
 except Exception as e:
        print(e, 'Error accessing Elasticsearch')
        
  for entry in response.to_dict()['hits']['hits']: 
    print(entry)  #here entry is json response event returned by search query
 
  #To access key-value from entry use, eg:
      entry['root_xwf__id]  #to get the id of root workflow
      entry['_source']['user'] # to get the user of a workflow
 ```

**Getting processed event**
The method in script: "get_processed_event()" has code retrieves the event from "pegasus-composite-event-\*" index and returns the processed event indexed in "processed-composite-workflow-events-\*". The paramters derived for processed event
can be found in the link:
<https://docs.google.com/document/d/1j5unwMUpc588cV5a_G9g664KLj_D-w12z12G602aVhA/edit>

A sample example of pegasus-composite event

```
 @timestamp	Apr 29, 2019 @ 21:07:15.000
t  _id	25519
t  _index	processed-composite-workflow-events2019.04.30
#  _score	1
t  _type	pegasus-composite-events-
t  actual_checksum	ea14a28e3edd89a8ecb92eea19b4f17a99d2bf7dd2a831e7d935234fda4c1589
#  checksum_failure	1
t  dest_proto_host	
#  end_time	1,556,597,278
#  executable	1
t  execution_hostname	node0481.palmetto.clemson.edu
t  execution_site	condorpool_largemem
t  expected_checksum	0156511ab39ec57ed7d68c3788ae83fe37bc8c485ef6a85dc1e2b4cb33ca1686
t  filename	DAT2071613.telescope.bz2
t  job_exitcode	256
t  job_id	job-wrapper_sh_ID0022557
t  job_type	1
#  local_dur	43
t  origin_id	c646cb7ae340b1aa63b229214da4a13b13d75f1e
#  retry_attempt	1
t  root_xwf_id	7eb7e6f6-d5f9-4485-9f17-e1707f7f069b
t  source_id	c646cb7ae340b1aa63b229214da4a13b13d75f1e
t  source_proto_host	
#  start_time	1,556,597,235
t  submit_hostname	login.uchicago.ci-connect.net
t  user_remote	agent3
t  user_submit	agent3
```


           
