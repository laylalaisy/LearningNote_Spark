## Spark Application - execute with spark-submit
 
## Imports
from __future__ import print_function
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
 
## Module Constants
APP_NAME = "My Spark Application"
 
## Closure Functions
def minmax(iterator):
    firsttime = 0
    for x in iterator:
        if(firsttime == 0):
            min = x
            max = x
            firsttime = 1
        else:
            if x > max:
                max = x
            if x < min:
                min = x
    return(min, max)

def f(iterator):
    for x in iterator:
        print(x)
    print('===')
 
## Main functionalitya
 
def main(sc):
    pass
 
if __name__ == "__main__":
   	# create an instance of a SparkSession as spark
    spark = SparkSession.builder.appName("wordcount").getOrCreate()

    # create SparkContext as sc
    sc = spark.sparkContext

    data = [10, 20, 3, 4, 5, 2, 2, 20, 20, 10]
    rdd = sc.parallelize(data, 3)
    print(rdd.getNumPartitions())
    print(rdd.collect())

    rdd.foreachPartition(f)

    minmaxlist = rdd.mapPartitions(minmax).collect()
    print(minmaxlist)
    print(min(minmaxlist))
    print(max(minmaxlist))

    # done!
    spark.stop()
