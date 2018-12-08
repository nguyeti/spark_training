"""SimpleApp.py"""

from pyspark import SparkContext

logFile = r"/usr/local/spark/README.md"
sc = SparkContext("local", "Simple App")

logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s : "a" in s).count()
numTs = logData.filter(lambda s : "t" in s).count()

print("Lines with a {0}, Lines with t {1}, test {0}".format(numAs,numTs))
