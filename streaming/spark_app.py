import json
import requests
from collections import Counter
from datetime import datetime
import time
import traceback
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import avg

BATCH_INTERVAL = 60
LANGAUGES = ['Python', 'Java', 'JavaScript']

# Dictionary to store all repositories sent by the Data Source service so far
# Each key is a repo id and value is their json data
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
    getBatchRepositories().append({
        'time': time,
        'repos': repositories
    })

def getSparkSession():
    return globals()['spark_session']

def getUpTime():
    return int(time.time() - globals()['startTime'])


def sendToClient(data):
    url = 'http://webapp:5000/updateData'
    requests.post(url, json=data)

def getTotalRepoCountByLanguage():
    repositories = getAllRepositories()

    # Convert the repositories dictionary to a list of dictionaries
    repos_list = list(repositories.values())

    # get the SparkSession object
    spark = getSparkSession()
    
    # Create a Spark DataFrame from the list of dictionaries
    df = spark.createDataFrame(repos_list)

    # Group the DataFrame by the 'language' column and count the number of occurrences of each language
    counts_df = df.groupBy('language').count()

    # Show the counts_df DataFrame in the console
    print("----------- REQUIREMENT 3.1 -----------")
    counts_df.show()

    # Convert the resulting DataFrame to a list of dictionaries
    counts_list = counts_df.rdd.map(lambda row: {'language': row['language'], 'count': row['count']}).collect()

    return counts_list

def getBatchedRepoLanguageCountsLast60Seconds():
    # Get repos that were pushed during the last 60 secodns
    sc = globals()['SparkContext']

    # Get latest /current batch to process
    current_batch = getBatchRepositories()[-1]
    batch_time = current_batch['time']

    # Using a global so I don't have to re-calculate batches
    if 'BatchedRepoLanguageCounts' not in  globals():
        globals()['BatchedRepoLanguageCounts'] = []       
    batched_repos_language_counts = globals()['BatchedRepoLanguageCounts']

    current_batch_repos_last_60 = {}
    # Get the repos that were pushed in the last 60 seconds
    for repo in current_batch['repos'].values():

        # Calculate the time passed since repo was pushed, store it in delta
        pushed_at = datetime.strptime(repo['pushed_at'], '%Y-%m-%dT%H:%M:%SZ')
        delta = batch_time - pushed_at

        # Check if the time difference is less than 60 seconds
        if delta.total_seconds() <= 60:
            if(repo['id'] not in current_batch_repos_last_60):
                current_batch_repos_last_60[repo['id']] = repo
    
    # Convert current_batch_repos_last_60 to an RDD
    repos = sc.parallelize(current_batch_repos_last_60.values())

    # Perform a map reduce to count the occurances of each langauge and collect it as a list
    counts  = repos.map(lambda repo: (repo['language'], 1)).reduceByKey(lambda a, b: a+b)
    counts_list = counts.map(lambda x: {"language": x[0], "count": x[1]}).collect()

    # Conver the RDD to a dictionary so I can easily check if a langauge is missing by making it the key 
    current_batch_language_counts = {count["language"]: count["count"] for count in counts_list}

    # Fill in missing data if needed
    # Iterate over the required languages list and add missing languages to the dictionary current_batch_language_counts.
    # I need to do this because I cannot gurantee that all three langauges will be represented in current_batch_language_counts
    # because I am further filtering them based on 'pushed_at' attribute.
    for language in LANGAUGES:
        if language not in current_batch_language_counts:
            current_batch_language_counts[language] = 0

    # Convert the dictionary back to a list of JSON objects and add them to batched_repos_language_counts list
    for language in current_batch_language_counts:
        batched_repos_language_counts.append({'batch_time': batch_time.strftime('%H:%M:%S'), "language": language, "count": current_batch_language_counts[language]})

    # get the SparkSession object
    spark = getSparkSession()
    
    # Create a Spark DataFrame from the list of dictionaries
    df = spark.createDataFrame(batched_repos_language_counts)

    # Show the df DataFrame in the console
    print("----------- REQUIREMENT 3.2 -----------")
    df.show()

    return batched_repos_language_counts

def getAvgStargazersByLanguage():
    repositories = getAllRepositories()

    # Convert the repositories dictionary to a list of dictionaries
    repos_list = list(repositories.values())

    # Get the SparkSession object
    spark = getSparkSession()

    # Create a Spark DataFrame from the list of dictionaries
    df = spark.createDataFrame(repos_list)

    # Group the DataFrame by the 'language' column and calculate the average 'stargazers_count' and set the new column name to 'avg_stargazers_count'
    avg_df = df.groupBy('language').agg(round(avg("stargazers_count"), 2).alias('avg_stargazers_count'))

    # Show the avg_df DataFrame in the console
    print("----------- REQUIREMENT 3.3 -----------")
    avg_df.show()

    # Convert the resulting DataFrame to a list of dictionaries
    avg_list = avg_df.rdd.map(lambda row: {'language': row['language'], 'avg_stargazers_count': row['avg_stargazers_count']}).collect()

    return avg_list

def getTopTenFrequentWordsByLanguage():
    repositories = getAllRepositories()
    sc = globals()['SparkContext']
    # Convert the repositories dictionary to a RDDs
    repos = sc.parallelize(repositories.values())

    # Split the description into words and group them by language
    # I use groupByKey to get elements where each element is a pair consisting of a language and an iterable of the words.
    words_by_language = repos.flatMap(lambda repo: ((repo['language'], word) for word in re.sub('[^a-zA-Z ]', '', str(repo['description'])).lower().split() if repo['description'] is not None) ) \
                             .groupByKey()
    
    # Count the frequency of each word
    # We first transform each language's iterable of the words to a Counter object with those words.
    # We then sort the resulting dictionary items by their values(frequency) in descending order and takes the top 10 items.
    # The result is elements where each element is a pair consisting of a language and the top ten most frequent words with their frequency.
    word_count_by_language = words_by_language.mapValues(lambda words: Counter(words)) \
                                               .mapValues(lambda word_count: sorted(word_count.items(), key=lambda x: x[1], reverse=True)[:10])

    # Collect the results
    top_words_by_language = word_count_by_language.collect()


    # Show results in Console
    print("----------- REQUIREMENT 3.4 -----------")
    spark = getSparkSession()
    top_words_by_language_list = []

    for language, top_words in top_words_by_language:
        top_words_by_language_list.append(
            {
            'language': language,
            'top_ten_words': top_words
            }
        )
    df = spark.createDataFrame(top_words_by_language_list)
    df.show()

    return top_words_by_language_list

def generateDataTxt(data):
    print("----------- DATA ANALYSIS (for data.txt) -----------")
     
    start_time = globals()['startTime']
    current_time = int(time.time())
    
    print("{}:{}".format(start_time, current_time))

    for i in range(0, 3):
        language = data['req1'][i]['language']
        repoCount = data['req1'][i]['count']
        avgStars = data['req3'][i]['avg_stargazers_count']
        print("{}:{}:{}".format(language, repoCount, avgStars))
    
    for d in data['req4']:
        language = d['language']
        top_ten_words =  ','.join(['({},{})'.format(item[0], item[1]) for item in d['top_ten_words']])
        print("{}:{}".format(language, top_ten_words))

def generateData():

    data = {
        'req1': getTotalRepoCountByLanguage(),
        'req2': getBatchedRepoLanguageCountsLast60Seconds(),
        'req3': getAvgStargazersByLanguage(),
        'req4': getTopTenFrequentWordsByLanguage()
    }

    sendToClient(data)
    generateDataTxt(data)

def processRdd(time, rdd):
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

        generateData()

    except Exception as e:
        # print("An error occurred:", e)
        # tb_str = traceback.format_tb(e.__traceback__)
        # print(f"Error traceback:\n{tb_str}")
        print('Waiting for Data...')

if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999

    sc = SparkContext(appName="EECS4415_Porject_3")
    globals()['SparkContext'] = sc

    sparkSession = SparkSession.builder.appName("EECS4415_Porject_3") .getOrCreate()
    globals()['spark_session'] = sparkSession

    print("Started Running....")
    globals()['startTime'] = int(time.time())

    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint("checkpoint_EECS4415_Porject_3")

    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    repos = data.flatMap(lambda json_str: [json.loads(json_str)])
    repos.foreachRDD(processRdd)
    ssc.start()
    ssc.awaitTermination()

