from operator import add

from pyspark.sql import SparkSession

def main():
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text("hdfs:///user/apathak2/input/assignment-1/stateoftheunion1790-2021.txt").rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()


if __name__ == "__main__":
    main()
