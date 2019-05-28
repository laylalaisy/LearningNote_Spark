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

    nums = [10, 1, 2, 9, 3, 4, 5, 6, 7]
    # ascending top 3
    mins = sc.parallelize(nums).takeOrdered(3)
    print(mins)
    # descending top 3
    maxs = sc.parallelize(nums).takeOrdered(3, key=lambda x: -x)
    print(maxs)

    # pair
    kv = [(10,"z1"), (1,"z2"), (2,"z3"), (9,"z4"), (3,"z5"), (4,"z6"), (5,"z7"), (6,"z8"), (7,"z9")]
    # ascending
    mins_pair = sc.parallelize(kv).takeOrdered(3)
    print(mins_pair)
    # descending
    maxs_pair = sc.parallelize(kv).takeOrdered(3, key=lambda x: -x[0])
    print(maxs_pair)

    # done!
    spark.stop()
