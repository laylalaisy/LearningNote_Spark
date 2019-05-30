## Spark Application - execute with spark-submit
 
## Imports
from __future__ import print_function
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
 
## Module Constants
APP_NAME = "My Spark Application"
 
## Closure Functions
def f(iterator):
    for x in iterator:
        print(x)
    print('===')

def adder(iterator):
    yield sum(iterator)
 
## Main functionalitya
 
def main(sc):
    pass
 
if __name__ == "__main__":
   	# create an instance of a SparkSession as spark
    spark = SparkSession.builder.appName("wordcount").getOrCreate()

    # create SparkContext as sc
    sc = spark.sparkContext

    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    rdd = sc.parallelize(numbers, 3)
    print(rdd.collect())

    print("Num Partitions:")
    print(rdd.getNumPartitions())
    print("\n")

    rdd.foreachPartition(f)

    print(rdd.mapPartitions(adder).collect())

    # done!
    spark.stop()
