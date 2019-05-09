

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

A sample example of pegasus-composite event :

| Parameter  | Value|
| ---------- | ------ |
| @timestamp |  Apr 29, 2019 @ 21:07:15.000 |
| @version   |		1                  | 
| \_id       |  c646cb7ae340b1aa63b229214da4a13b13d75f1e
| \_index    |  pegasus-composite-events-2019.04
| \_score    |	 - 
| \_type     |  doc
| dag	     |  gropticsANDcare-0.dag
| dax	     |  /home/agent3/git/VERITAS/GrOpticsAndCareCombined/dax.xml
| event	     | stampede.job_inst.composite
| exitcode   | 256
| int_error_count | 1 |
| job__id |job-wrapper_sh_ID0022557 |
|job_inst__id | 84,173 |
| jobtype | 1
| js__id  | 	4
| level |	Error
| local__dur |	43
| multiplier_factor | 1
| pegasus__version | 4.9.2dev
| root__xwf__id	| 7eb7e6f6-d5f9-4485-9f17-e1707f7f069b
| sched__id	| 6459517.0
| site	| condorpool_largemem
| status	| -1
stderr__file |	01/F6/job-wrapper_sh_ID0022557.err.000
stderr__text |	2019-04-30 00:06:33: PegasusLite: version 4.9.2dev%0A2019-04-30 00:06:33: Executing on host node0481.palmetto.clemson.edu OSG_SITE_NAME=osg-ce GLIDEIN_Site=Clemson GLIDEIN_ResourceName=Clemson-Palmetto%0A2019-04-30 00:06:33: Expanded $S3CFG_staging to /srv/s3cfg%0A%0A########################[Pegasus Lite] Setting up workdir ########################%0A2019-04-30 00:06:33: Checking /srv for potential use as work space... %0A2019-04-30 00:06:33:   Workdir is /srv/pegasus.VbNX6GPik - 20G available%0A2019-04-30 00:06:33: Changing cwd to /srv/pegasus.VbNX6GPik%0A%0A##############[Pegasus Lite] Figuring out the worker package to use ##############%0A2019-04-30 00:06:33: The job contained a Pegasus worker package%0A2019-04-30 00:06:34: Warning: worker package pegasus-worker-4.9.2dev-x86_64_rhel_7.tar.gz does not seem to match the system x86_64_rhel_6%0A2019-04-30 00:06:34: Warning: Pegasus binaries in /usr/bin do not match Pegasus version used for current workflow%0A2019-04-30 00:06:34: Using /cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/worker/4.9.2dev/x86_64_rhel_6 as worker package%0A%0A###################### Staging in input data and executables ######################%0A2019-04-30 00:06:35,064    INFO:  Reading URL pairs from stdin%0A2019-04-30 00:06:35,067    INFO:  3 transfers loaded%0A2019-04-30 00:06:35,067    INFO:  PATH=/cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/worker/4.9.2dev/x86_64_rhel_6/bin:/cvmfs/oasis.opensciencegrid.org/osg/modules/lua/bin:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:/software/osg-wn-client/usr/bin%0A2019-04-30 00:06:35,067    INFO:  LD_LIBRARY_PATH=/host-libs:/software/osg-wn-client/usr/lib%0A2019-04-30 00:06:35,184    INFO:  --------------------------------------------------------------------------------%0A2019-04-30 00:06:35,184    INFO:  Starting transfers - attempt 1%0A2019-04-30 00:06:37,188    INFO:  Tool found: pegasus-s3   Version: N/A   Path: /cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/worker/4.9.2dev/x86_64_rhel_6/bin/pegasus-s3%0A2019-04-30 00:06:37,188    INFO:  /cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/worker/4.9.2dev/x86_64_rhel_6/bin/pegasus-s3 get %27s3://agent3@osgconnect/agent3/intermediate/wf-1556036485/00/00/GrOpticsAndCare.tar.gz%27 %27/srv/pegasus.VbNX6GPik/GrOpticsAndCare.tar.gz%27%0A2019-04-30 00:06:39,628    INFO:  /cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/worker/4.9.2dev/x86_64_rhel_6/bin/pegasus-s3 get %27s3://agent3@osgconnect/agent3/intermediate/wf-1556036485/00/00/job-wrapper.sh%27 %27/srv/pegasus.VbNX6GPik/job-wrapper.sh%27%0A2019-04-30 00:06:40,208    INFO:  /cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/worker/4.9.2dev/x86_64_rhel_6/bin/pegasus-s3 get %27s3://agent3@osgconnect/agent3/intermediate/wf-1556036485/01/F6/DAT2071613.telescope.bz2%27 %27/srv/pegasus.VbNX6GPik/DAT2071613.telescope.bz2%27%0A2019-04-30 00:07:11,677    INFO:  --------------------------------------------------------------------------------%0A2019-04-30 00:07:11,678    INFO:  Stats: Total 3 transfers, 102.9 MB transferred in 37 seconds. Rate: 2.8 MB/s (22.5 Mb/s)%0A2019-04-30 00:07:11,678    INFO:         Between sites staging->condorpool_largemem : 3 transfers, 102.9 MB transferred in 37 seconds. Rate: 2.8 MB/s (22.5 Mb/s)%0A2019-04-30 00:07:11,678    INFO:  All transfers completed successfully.%0A%0A##################### Setting the xbit for executables staged #####################%0A%0A##################### Checking file integrity for input files #####################%0AIntegrity check: DAT2071613.telescope.bz2: Expected checksum (0156511ab39ec57ed7d68c3788ae83fe37bc8c485ef6a85dc1e2b4cb33ca1686) does not match the calculated checksum (ea14a28e3edd89a8ecb92eea19b4f17a99d2bf7dd2a831e7d935234fda4c1589) (timing: 0.892)%0A%0A2019-04-30 00:07:12: Last command exited with 1%0A2019-04-30 00:07:12: /srv/pegasus.VbNX6GPik cleaned up%0APegasusLite: exitcode 1%0A
| stdout__file |	01/F6/job-wrapper_sh_ID0022557.out.000
| stdout__text	| #@ 1 stdout%0A
| submit__dir	| /local-scratch/agent3/workflows/wf-1556036485
| submit__hostname |	login.uchicago.ci-connect.net
| ts	| 1,556,597,235
| user	| agent3
| wf__ts	| 1,556,076,718
| wf_user	| agent3
| work_dir	| /local-scratch/agent3/workflows/wf-1556036485
| xwf__id	| 7eb7e6f6-d5f9-4485-9f17-e1707f7f069b

The corresponding processed event is: 

|Parameter| Value|
|---------| ---- |
|timestamp| Apr 29, 2019 @ 21:07:15.000
| \_id|25519
|\_index |	processed-composite-workflow-events2019.04.30
|\_score |	1
|\_type |	pegasus-composite-events-
| actual_checksum| ea14a28e3edd89a8ecb92eea19b4f17a99d2bf7dd2a831e7d935234fda4c1589
|checksum_failure |	1
|dest_proto_host |	
| end_time | 1,556,597,278
| executable |	1
| execution_hostname |	node0481.palmetto.clemson.edu
| execution_site  | 	condorpool_largemem
| expected_checksum  |	0156511ab39ec57ed7d68c3788ae83fe37bc8c485ef6a85dc1e2b4cb33ca1686
| filename |	DAT2071613.telescope.bz2
| job_exitcode | 256
| job_id |	job-wrapper_sh_ID0022557
| job_type | 1
| local_dur|	43
|  origin_id |	c646cb7ae340b1aa63b229214da4a13b13d75f1e
|  retry_attempt | 1
| root_xwf_id	| 7eb7e6f6-d5f9-4485-9f17-e1707f7f069b
|  source_id	| c646cb7ae340b1aa63b229214da4a13b13d75f1e
|  source_proto_host |	
|  start_time	| 1,556,597,235
| submit_hostname | login.uchicago.ci-connect.net
| user_remote	| agent3
|  user_submit	| agent3
```
