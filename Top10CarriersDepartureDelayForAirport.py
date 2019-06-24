from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructField

import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Not enough arguments")
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Top Carriers For Each Airport") \
        .getOrCreate()

    airportarg = sys.argv[1]

    file = spark.read.text(sys.argv[2])

    remove_quotes = lambda airport_string: airport_string.replace('"', '')


    def is_filtered_airport(airport):
        return len(airport) == 3 and airport[0] == airportarg


    def split_airport_and_airport_string_into_origin_aiport_and_dest_airport_tuple(airport_and_airport_string, average_delay):

        [airport, carrier] = airport_and_airport_string.split(" ")
        return [airport, carrier, average_delay]


    top10AirportsPerAirportSchema = StructType([StructField('Origin Airport', StringType(), True), StructField('Carrier', StringType(), True), StructField('Average Arrival Delay', FloatType(), True)])

    lines = file.rdd \
        .cache() \
        .keys() \
        .map(lambda l: l.split("\t")) \
        .map(lambda t: [remove_quotes(t[0]), float(t[1])]) \
        .map(lambda t: split_airport_and_airport_string_into_origin_aiport_and_dest_airport_tuple(t[0], t[1])) \
        .filter(lambda t: is_filtered_airport(t))

    df = spark \
        .createDataFrame(lines, schema=top10AirportsPerAirportSchema) \
        .orderBy(["Origin Airport", "Average Arrival Delay"])

    df.coalesce(1) \
        .write \
        .option("header", "true") \
        .csv(sys.argv[3], header=True)
