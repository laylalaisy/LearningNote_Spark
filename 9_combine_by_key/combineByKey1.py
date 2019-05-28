# rdd2 = rdd.combineByKey(createCombiner, mergeValue, mergeCombiners) 
# 1. createCombiner, which turns a V into a C (e.g., creates a one-element list)
#    V --> C
# 2. mergeValue, to merge a V into a C (e.g., adds it to the end of a list)
#    C, V --> C
# 3. mergeCombiners, to combine two C’s into a single one.
#    C, C --> C

# https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/combine-by-key/spark-combineByKey.md

 
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

    data = [("A", 2.), ("A", 4.), ("A", 9.), ("B", 10.), ("B", 20.), ("Z", 3.), ("Z", 5.), ("Z", 8.), ("Z", 12.)]

    rdd = sc.parallelize(data)

    sumCount = rdd.combineByKey(lambda value: (value, 1),
                            lambda x, value: (x[0] + value, x[1] + 1),
                            lambda x, y: (x[0] + y[0], x[1] + y[1])
                           )
    print(sumCount)

    # multiple value
    averageByKey = sumCount.map(lambda key_vals: (key_vals[0], key_vals[1][0] / key_vals[1][1]))
    average = averageByKey.collectAsMap()
    print(average)

    # done!
    spark.stop()
