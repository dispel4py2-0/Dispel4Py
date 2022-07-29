
## How to run the Astrophysics: Internal Extinction of Galaxies test

To run the this test, first you need to install:
```bash
$ pip install requests
$ pip install astropy
``` 

Then, run the script. Example run commands for several other modes are listed below:

### Run with simple mode:
```bash
$ dispel4py\new\processor.py simple dispel4py.examples.internal_extinction.int_ext_graph -d "{\"read\" : [ {\"input\" : \"coordinates.txt\"} ]}"
```
The ‘coordinates.txt’ file is the workflow's input data with the coordinates of the galaxies.

### Run with multiprocessing mode:
```bash
$ dispel4py\new\processor.py multi dispel4py.examples.internal_extinction.int_ext_graph -n 4 -d "{\"read\" : [ {\"input\" : \"coordinates.txt\"} ]}"
``` 
 Parameter '-n' specify the number of processes.

### Run with old dynamic mode:
```bash
$ dispel4py\new\processor.py dynamic dispel4py.examples.internal_extinction.int_ext_graph  -n 4 -d "{\"read\" : [ {\"input\" : \"coordinates.txt\"} ]}"
``` 
### Run with dynamic v1 mode:
```bash
$ dispel4py\new\processor.py dynamic_redis_v1 dispel4py.examples.article_sentiment_analysis.analysis_sentiment -ri localhost -n 4 -d "{\"read\" : [ {\"input\" : \"Articles_cleaned.csv\"} ]}"
```
Note that, you should additionally specify the IP address(parameter '-ri') and port(parameter '-rp', optional, default 6379) of the redis server.
  
### Run with enhanced dynamic mode:
```bash
$ dispel4py\new\processor.py dynamic_redis dispel4py.examples.article_sentiment_analysis.analysis_sentiment -ri localhost -n 12 -d "{\"read\" : [ {\"input\" : \"Articles_cleaned.csv\"} ]}"
```
