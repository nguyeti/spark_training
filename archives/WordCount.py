"""WordCount.py"""

from pyspark import SparkContext
import re

logFile = r"/usr/local/spark/README.md"
sc = SparkContext("local","Word Count")

logData = sc.textFile(logFile).cache()

# RDD, separated words
# words = logData.flatMap(lambda line : line.split())
# (I, 1) (love, 1) (love,1)
# mapper = words.map(lambda word : (word, 1))
# (I,1) (love, (1,1))
# (I,1) (love, 2)
# counts = mapper.reduceByKey(lambda a,b : a + b)


# one liner
# counts = logData.flatMap(lambda line : line.split()).map(lambda word : (word,1)).reduceByKey(lambda a,b : a + b)
# counts.saveAsTextFile("WordCountOutput")


# regex 
# counts = logData.flatMap(lambda line : re.compile(r"[\w']+").findall(line)).map(lambda word : (word,1)).reduceByKey(lambda a,b : a + b)
# counts.saveAsTextFile("WordCountRegexOutput")

# regex + sorted
counts = logData.flatMap(lambda line : re.compile(r"[\w']+").findall(line)).map(lambda word : (word,1)).reduceByKey(lambda a,b : a + b).sortByKey(True)
counts.saveAsTextFile("WordCountRegexSortedOutput")
