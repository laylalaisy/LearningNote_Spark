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

    nums = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 20])
    print(nums.collect())

    sumAndCount = nums.map(lambda x: (x, 1)).fold((0, 0), (lambda x, y: (x[0]+y[0], x[1]+y[1])))
    print(sumAndCount)

    avg = float(sumAndCount[0]) / float(sumAndCount[1])
    print(avg)
    # done!
    spark.stop()
