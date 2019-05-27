## Spark Application - execute with spark-submit
 
## Imports
from __future__ import print_function
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
 
## Module Constants
APP_NAME = "My Spark Application"
 
## Closure Functions
 
## Main functionalitya
 
def main(sc):
    pass
 
if __name__ == "__main__":
   	# create an instance of a SparkSession as spark
    spark = SparkSession.builder.appName("wordcount").getOrCreate()

    # create SparkContext as sc
    sc = spark.sparkContext

    inputPath = './data.txt'

    # create RDD from a text file
    lines= sc.textFile(inputPath)
    print(lines.collect())

    bigrams = lines.map(lambda s : s.split(" ")).flatMap(lambda s: [((s[i],s[i+1]),1) for i in range (0, len(s)-1)])
    print(bigrams.collect())

    counts = bigrams.reduceByKey(lambda x, y: x+y)
    print(counts.collect())

    # done!
    spark.stop()
