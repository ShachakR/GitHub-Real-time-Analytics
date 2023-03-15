import json
import requests
import sys
from datetime import datetime
import traceback
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

BATCH_INTERVAL = 15

# Where each key is a repo id and value is their json data
def getAllRepositories() -> dict:
    if('repositories' not in globals()):
        globals()['repositories'] = {}
    return globals()['repositories']

def updateAllRepositories(repositories):
    globals()['repositories'] = repositories

def getBatchRepositories() -> list:
    if('batch_repositories' not in globals()):
        globals()['batch_repositories'] = []
    return globals()['batch_repositories']

def updateBatchRepositories(repositories):
    time = datetime.utcnow()
    batch_time = time.strftime('%H:%M:%S')

    getBatchRepositories().append({
        'time': batch_time,
        'repos': repositories
    })

def getUpTime():
    if('upTime' not in globals()):
        globals()['upTime'] = 0

    uptime = globals()['upTime']
    globals()['upTime'] = uptime + BATCH_INTERVAL
    return uptime


def send_to_client(data):
    url = 'http://webapp:5000/updateData'
    requests.post(url, json=data)

def count_language_repos_total():
    sc = globals()['SparkContext']
    repositories = getAllRepositories()

    # creating RDDs from list 
    repos = sc.parallelize(repositories.values())
    counts  = repos.map(lambda repo: (repo['language'], 1)).reduceByKey(lambda a, b: a+b)
    counts_list = counts.map(lambda x: {"language": x[0], "count": x[1]})
    counts_json_data = json.dumps(counts_list.collect())
    return counts_json_data

def count_batched_repos_last_60():
    # get repos that were pushed during the last 60 secodns
    sc = globals()['SparkContext']
    batched_repositories = getBatchRepositories()

    data = []
    
    for batch in batched_repositories:
        repos_last_60 = {}
        for repo in batch['repos'].values():
            pushed_at = datetime.strptime(repo['pushed_at'], '%Y-%m-%dT%H:%M:%SZ')
            time = datetime.utcnow()
            delta = time - pushed_at
            # check if the time difference is less than 60 seconds
            if delta.total_seconds() <= 60:
                if(repo['id'] not in repos_last_60):
                    repos_last_60[repo['id']] = repo
        
        repos = sc.parallelize(repos_last_60.values())
        counts  = repos.map(lambda repo: (repo['language'], 1)).reduceByKey(lambda a, b: a+b)
        counts_list = counts.map(lambda x: {"language": x[0], "count": x[1]})
        counts_json_data = json.dumps(counts_list.collect())

        data.append({
            'batch_time': batch['time'],
            'batch_counts': counts_json_data
            })

    return data

def displayDataToConsole(data):
    print("----------- REQUIREMENT 3.1 -----------")
    print(data['req1'])

    print("----------- REQUIREMENT 3.2 -----------")
    print(data['req2'])

def generate_data():
    data = {
        'req1': count_language_repos_total(),
        'req2': count_batched_repos_last_60()
    }

    send_to_client(data)
    displayDataToConsole(data)

def process_rdd(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    print("Up time: %s seconds" % str(getUpTime()))
    try:
        
        repositories = getAllRepositories() 
        batch_repositories = {}

        # Store each repo in the RDD in the repositories (all), and batch_repositories (this batch)
        # Each repo is stored only once
        for repo in rdd.collect():
            if(repo['id'] not in batch_repositories):
                batch_repositories[repo['id']] = repo

            if(repo['id'] not in repositories):
                repositories[repo['id']] = repo

        # Update the stored repositories
        updateAllRepositories(repositories)
        updateBatchRepositories(batch_repositories)

        generate_data()

    except Exception as e:
        print("An error occurred:", e)
        tb_str = traceback.format_tb(e.__traceback__)
        print(f"Error traceback:\n{tb_str}")

if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="EECS4415_Porject_3")
    globals()['SparkContext'] = sc
    print("Started Running....")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint("checkpoint_EECS4415_Porject_3")

    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    repos = data.flatMap(lambda json_str: [json.loads(json_str)])
    repos.foreachRDD(process_rdd)
    ssc.start()
    ssc.awaitTermination()

