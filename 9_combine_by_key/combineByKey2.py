## Imports
from __future__ import print_function
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

import math
 
## Module Constants
APP_NAME = "My Spark Application"
 
## Closure Functions
def stdDev(sumX, sumSquared, n):
    mean = sumX / n
    stdDeviation = math.sqrt((sumSquared - n*mean*mean) / n)
    return(mean, stdDeviation)

## Main functionalitya
 
def main(sc):
    pass
 
if __name__ == "__main__":
   	# create an instance of a SparkSession as spark
    spark = SparkSession.builder.appName("wordcount").getOrCreate()

    # create SparkContext as sc
    sc = spark.sparkContext

    data = [("A", 2.), ("A", 4.), ("A", 9.), ("B", 10.), ("B", 20.), ("Z", 3.), ("Z", 5.), ("Z", 8.), ("Z", 12.)]

    rdd = sc.parallelize(data)
    print(rdd.collect())
    print(rdd.count())

    # mean and standard deviation
    sumCount = rdd.combineByKey(lambda value: (value, value*value, 1),
                            lambda x, value: (x[0] + value, x[1] + value*value, x[2]+1),
                            lambda x, y: (x[0] + y[0], x[1] + y[1], x[2]+y[2])
                           )
    print(sumCount.collect())

    meanAndStdDev = sumCount.mapValues(lambda x: stdDev(x[0], x[1], x[2]))
    print(meanAndStdDev.collect())

    # done!
    spark.stop()
