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

    nums = sc.parallelize([1, 2, 3, 4, 5, 6, 7])
    nums.collect()

    filtered1 = nums.filter(lambda x: x%2 == 1)
    print(filtered1.collect())

    filtered2 = nums.filter(lambda x: x%2 == 0)
    print(filtered2.collect())

    # done!
    spark.stop()
