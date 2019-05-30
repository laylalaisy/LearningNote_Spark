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

    a = [('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
    rdd = sc.parallelize(a)
    print(rdd.collect())

    sorted = rdd.sortByKey()
    print(sorted.collect())

    rdd2 = rdd.map(lambda x: (x[1], x[0]))
    print(rdd2.collect())

    sorted = rdd2.sortByKey()
    print(rdd2.collect())

    sorted = rdd2.sortByKey(False)
    print(sorted.collect())

    indices = sorted.zipWithIndex()
    print(indices.collect())

    # done!
    spark.stop()
