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

    R = sc.textFile("R.txt");
    print(R.collect())

    S = sc.textFile("S.txt");
    print(S.collect())

    r1 = R.map(lambda s: s.split(","))
    print(r1.collect())

    r2 = r1.flatMap(lambda s: [(s[0], s[1])])
    print(r2.collect())

    s1 = S.map(lambda s: s.split(","))
    print(s1.collect())

    s2 = s1.flatMap(lambda s: [(s[0], s[1])])
    print(s2.collect())

    RjoinedS = r2.join(s2)
    print(RjoinedS.collect())

    # done!
    spark.stop()
