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

    a = [('k1','v1'), ('k2', 'v2')]
    b = [('k3','v3'), ('k4', 'v4'), ('k5', 'v5') ]

    rdd1= sc.parallelize(a)
    print(rdd1.collect())

    rdd2 = sc.parallelize(b)
    print(rdd2.collect())

    rdd3 = rdd1.cartesian(rdd2)
    print(rdd3.collect())

    # done!
    spark.stop()
