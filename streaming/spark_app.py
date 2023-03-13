import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

BATCH_INTERVAL = 15
repositories = {}

def extract_repository(json_str):
    # Parse JSON string to a dictionary
    json_obj = json.loads(json_str)
    # Extract repository name
    repo_id = json_obj["repo_id"]
    # Return tuple
    return (repo_id, json_obj)

def store_repositories(rdd):
    # Collect each partition of the RDD as a list
    repo_list = rdd.collect()
    # Store each repository in the dictionary
    for repo in repo_list:
        repo_id = repo[0]
        repo_data = repo[1]
        repositories[repo_id] = repo_data
        print(repositories[repo_id])

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
    ssc.start()
    ssc.awaitTermination()

