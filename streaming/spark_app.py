import json
import requests
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

def send_to_client(data):
    url = 'http://webapp:5000/updateData'
    requests.post(url, json=data)

def extract_repository(json_str):
    # Parse JSON string to a dictionary
    json_obj = json.loads(json_str)
    # Extract repository name
    repo_id = json_obj["id"]
    # Return tuple
    return (repo_id, json_obj)

def store_repositories(rdd):
    # Collect each partition of the RDD as a list
    repo_list = rdd.collect()
    # Store each repository in the dictionary
    repositories = getStoredRepositories()
    for repo in repo_list:
        print(repo)
        repo_id = repo[0]
        repo_data = repo[1]
        repositories[repo_id] = repo_data


def count_language_repos_total(repos, sc):
    counts  = repos.map(lambda repo: (repo['language'], 1)).reduceByKey(lambda a, b: a+b)
    counts_list = counts.map(lambda x: {"language": x[0], "count": x[1]})
    counts_json_data = json.dumps(counts_list.collect())
    return counts_json_data

if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="EECS4415_Porject_3")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint("checkpoint_EECS4415_Porject_3")

    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    repos = data.flatMap(lambda json_str: extract_repository(json_str))
    repos.foreachRDD(store_repositories)

    repositories = getStoredRepositories()
    rdd_repos = sc.parallelize(repositories.values())
    language_total_counts = count_language_repos_total(rdd_repos, sc)

    data = {
        'req1': language_total_counts
    }

    send_to_client(data)

    ssc.start()
    ssc.awaitTermination()

