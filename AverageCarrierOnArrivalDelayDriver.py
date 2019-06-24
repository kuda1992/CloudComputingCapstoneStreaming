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
        .appName("AverageCarrierOnArrivalDelay") \
        .getOrCreate()

    file = spark.read.text(sys.argv[1])

    def removequotes(airport_string):
        return airport_string.replace('"', '')


    top10AirportsSchema = StructType([StructField('Carrier', StringType(), True), StructField('Average On Arrival Delay', StringType(), True)])

    lines = file.rdd \
        .cache() \
        .keys() \
        .map(lambda l: l.split("\t")) \
        .map(lambda t: [removequotes(t[0]), float(t[1])]) \
        .sortBy(lambda t: t[1], ascending=False) \
        .map(lambda t: [t[0], str(t[1])]) \
        .take(10)

    df = spark \
        .createDataFrame(lines, schema=top10AirportsSchema)

    df.coalesce(1) \
        .write \
        .option("header", "true") \
        .csv(sys.argv[2], header=True)

    spark.stop()
