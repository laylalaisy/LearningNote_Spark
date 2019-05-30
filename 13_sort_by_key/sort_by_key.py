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

    lines = sc.textFile('data.txt')
    print(lines.collect())

    frequencies = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    print(frequencies.collect())
    print(frequencies.count())

    # ascending
    sorted = frequencies.sortByKey()
    print(sorted.collect())

    # descending
    sortedDescending = frequencies.sortByKey(False)
    print(sortedDescending.collect())

    # done!
    spark.stop()
