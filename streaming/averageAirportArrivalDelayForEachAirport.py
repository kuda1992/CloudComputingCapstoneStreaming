from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from streaming.utils.helpers import Helpers

import sys


def get_airport_carrier_and_departure_delay(line):
    cols = line.split(",")

    if len(cols) < 35:
        return {}
    carrier = Helpers.remove_quotes(cols[6])
    dest_airport = Helpers.remove_quotes(cols[24])
    airport = Helpers.remove_quotes(cols[14])
    departure_delay = Helpers.remove_quotes(cols[33])
    return {"airport": airport, "dest_airport": dest_airport, "carrier": carrier, "departure_delay": departure_delay}


def calculate_average(x):
    carrier = x[0]
    average_departure_delays = x[1]
    count = 0
    sum_of_delays = 0
    for a in average_departure_delays:
        if len(a) > 1:
            sum_of_delays = sum_of_delays + float(a)
            count = count + 1
    return carrier, sum_of_delays / count


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Not enough arguments")
        exit(-1)

    origin_airport_filter = sys.argv[1]
    dest_airport_filter = sys.argv[2]

    spark_context = SparkContext(appName="Top 10 Carriers Per Destination Pair")
    spark_context.setLogLevel("WARN")

    spark_streaming_context = StreamingContext(spark_context, 1)

    lines = spark_streaming_context.textFileStream(sys.argv[3])

    lines \
        .map(get_airport_carrier_and_departure_delay) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: Helpers.is_carrier(line) and Helpers.is_airportid(line)) \
        .filter(lambda line: line.get('airport') == origin_airport_filter and line.get('dest_airport') == dest_airport_filter) \
        .map(lambda line: (line.get('carrier'), line.get('departure_delay'))) \
        .groupByKey() \
        .map(calculate_average) \
        .transform(lambda carriers: carriers.sortBy(lambda t: t[1], ascending=True)) \
        .pprint(100)

spark_streaming_context.start()
spark_streaming_context.awaitTermination()
