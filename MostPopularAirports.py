from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField

import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Not enough arguments")
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Most Popular Airports") \
        .getOrCreate()

    file = spark.read.text(sys.argv[1])


    def remove_quotes(airport_string):
        return airport_string.replace('"', '')


    MostPopularAiportsSchema = StructType([StructField('Airport', StringType(), True), StructField('Number of Carriers', StringType(), True)])

    lines = file.rdd \
        .cache() \
        .keys() \
        .map(lambda l: l.split("\t")) \
        .map(lambda t: [remove_quotes(t[0]), float(t[1])]) \
        .sortBy(lambda t: t[1], ascending=False) \
        .map(lambda t: [t[0], str(t[1])]) \
        .collect()

    df = spark \
        .createDataFrame(lines, schema=MostPopularAiportsSchema)
    df.coalesce(1) \
        .write \
        .option("header", "true") \
        .csv(sys.argv[2], header=True)
