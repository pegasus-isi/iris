

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
 `    entry['root_xwf__id]  #to get the id of root workflow
      entry['_source']['user'] # to get the user of a workflow
 ```

** Getting processed event **
The method in script: "get_processed_event()" has code for retrieving the processed event. The paramters derived for processedevent
can be found in the link:
<https://docs.google.com/document/d/1j5unwMUpc588cV5a_G9g664KLj_D-w12z12G602aVhA/edit>



           
