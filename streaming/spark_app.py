import json
import requests
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

BATCH_INTERVAL = 15

# Where each key is a repo id and value is their json data
def getStoredRepositories():
    if('repositories' not in globals()):
        globals()['repositories'] = {}
    return globals()['repositories']

def updateStoredRepositories(repositories):
    globals()['repositories'] = repositories

def send_to_client(data):
    url = 'http://webapp:5000/updateData'
    requests.post(url, json=data)

def count_language_repos_total(repos):
    counts  = repos.map(lambda repo: (repo['language'], 1)).reduceByKey(lambda a, b: a+b)
    counts_list = counts.map(lambda x: {"language": x[0], "count": x[1]})
    counts_json_data = json.dumps(counts_list.collect())
    return counts_json_data

def generate_data():
    sc = globals()['SparkContext']
    repositories = getStoredRepositories()
    rdd_repos = sc.parallelize(repositories.values())
    language_total_counts = count_language_repos_total(rdd_repos)

    data = {
        'req1': language_total_counts
    }

    send_to_client(data)

def process_rdd(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        
        repositories = getStoredRepositories() 
        # Store each repo in the RDD in the repositories
        for repo in rdd.collect():
            repositories[repo['id']] = repo

        # Update the stored repositories
        updateStoredRepositories(repositories)

        generate_data()

    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="EECS4415_Porject_3")
    globals()['SparkContext'] = sc
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint("checkpoint_EECS4415_Porject_3")

    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    repos = data.flatMap(lambda json_str: [json.loads(json_str)])
    repos.foreachRDD(process_rdd)
    ssc.start()
    ssc.awaitTermination()

