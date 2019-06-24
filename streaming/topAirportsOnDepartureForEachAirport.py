from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from streaming.utils.helpers import Helpers

import sys


def get_airport_dest_airport_and_departure_delay(line):
    cols = line.split(",")

    if len(cols) < 35:
        return {}
    dest_airport = Helpers.remove_quotes(cols[24])
    airport = Helpers.remove_quotes(cols[14])
    departure_delay = Helpers.remove_quotes(cols[33])
    return {"airport": airport, "dest_airport": dest_airport, "departure_delay": departure_delay}


def calculate_average(x):
    airport = x[0]
    average_departure_delays = x[1]
    count = 0
    sum_of_delays = 0
    for a in average_departure_delays:
        if len(a) > 1:
            sum_of_delays = sum_of_delays + float(a)
            count = count + 1
    return airport, sum_of_delays / count


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Not enough arguments")
        exit(-1)

    airport_filter = sys.argv[1]

    spark_context = SparkContext(appName="Top 10 Airport For Each Airport on Departure Performances")
    spark_context.setLogLevel("WARN")

    spark_streaming_context = StreamingContext(spark_context, 1)

    lines = spark_streaming_context.textFileStream(sys.argv[2])

    lines \
        .map(get_airport_dest_airport_and_departure_delay) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: Helpers.is_airportid(line)) \
        .filter(lambda line: line.get('airport') == airport_filter) \
        .map(lambda line: (line.get('dest_airport'), line.get('departure_delay'))) \
        .groupByKey() \
        .map(calculate_average) \
        .transform(lambda carriers: carriers.sortBy(lambda t: t[1], ascending=True)) \
        .pprint(10)

spark_streaming_context.start()
spark_streaming_context.awaitTermination()
