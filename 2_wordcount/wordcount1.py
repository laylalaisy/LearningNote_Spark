## Spark Application - execute with spark-submit
 
## Imports
from pyspark import SparkConf, SparkContext
 
## Module Constants
APP_NAME = "My Spark Application"
 
## Closure Functions
 
## Main functionality
 
def main(sc):
    pass
 
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
 
    # Execute Main functionality
    lines = sc.textFile("data.txt", 1)
    debuglines = lines.collect()
    print("debuglines: ")
    print(debuglines)

    words = lines.flatMap(lambda x: x.split(' '))
    debugwords = words.collect()
    print("debugwords: ")
    print(debugwords)

    ones = words.map(lambda x: (x, 1))
    debugones = ones.collect()
    print("debugones: ")
    print(debugones)

    counts = ones.reduceByKey(lambda x, y: x + y)
    debugcounts = counts.collect()
    print("debugcounts: ")
    print(debugcounts)

    counts.saveAsTextFile("output")

    frequencies = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    debugfrequencies = frequencies.collect()
    print("debugfrequencies: ")
    print(debugfrequencies)
    count = frequencies.count()
    print(count)



