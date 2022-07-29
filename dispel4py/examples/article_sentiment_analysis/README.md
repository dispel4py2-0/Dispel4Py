
## How to run the test

To run this test, the following two steps are required, namely the preparation of the data and the execution of the test script.

### Preparation of data
In order to run this test, you must first prepare the article data needed for the test. We collect some article data from http://aaa.com and saved as "Articles.csv" in this repository. Before running the test, you must first run "clean.py" in this directory to clean the data. 

To run the data cleaning program, first you need to install:
```bash
$ pip install pandas
``` 

Then, run the clean script:
```bash
$ python clean.py Articles.csv
``` 

After cleaning, a new file named "Articles_cleaned.csv" will occur by default. This file is the input of the next step. 

Note that you don't need to run the cleaning script again if you already have the cleaned data.


### Execution of the test script

To run the test script, first you need to install:
```bash
$ pip install nltk numpy 
``` 

The workflow source code is "analysis_sentiment.py". You could modify the ROOT_DIR if you want to indicate a different folder.


In multiprocessing mode, parameter '-n' specify the number of processes. For executing it with the multiprocessing mode and assign 13 processes:
```bash
$ dispel4py\new\processor.py multi dispel4py.examples.article_sentiment_analysis.analysis_sentiment -n 13 -d "{\"read\" : [ {\"input\" : \"Articles_cleaned.csv\"} ]}"
``` 
Compared to multiprocessing mode, you will additionally need to specify the IP address and port(optional, default 6379) of the redis server .For executing it with the enhanced dynamic mode, connecting to local redis server, and assign 12 processes:
```bash
$ dispel4py\new\processor.py dynamic_redis dispel4py.examples.article_sentiment_analysis.analysis_sentiment -ri localhost -n 12 -d "{\"read\" : [ {\"input\" : \"Articles_cleaned.csv\"} ]}"
``` 

