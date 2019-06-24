from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from streaming.utils.helpers import Helpers

import sys


def get_carrier_and_arrival_delay(line):
    cols = line.split(",")

    if len(cols) < 35:
        return {}
    carrier = Helpers.remove_quotes(cols[6])
    average_delay = Helpers.remove_quotes(cols[44])
    return {"carrier": carrier, "average_delay": average_delay}


def calculate_average(x):
    carrier = x[0]
    average_delays = x[1]
    count = 0
    sum_of_delays = 0
    for a in average_delays:
        if len(a) > 1:
            sum_of_delays = sum_of_delays + float(a)
            count = count + 1
    return carrier, sum_of_delays / count


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Not enough arguments")
        exit(-1)

    spark_context = SparkContext(appName="Average Carrier On Arrival Delay")
    spark_context.setLogLevel("WARN")

    spark_streaming_context = StreamingContext(spark_context, 1)

    lines = spark_streaming_context.textFileStream(sys.argv[1])

    lines \
        .map(get_carrier_and_arrival_delay) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: Helpers.is_carrier(line)) \
        .map(lambda airport: (airport.get('carrier'), airport.get('average_delay'))) \
        .groupByKey() \
        .map(calculate_average) \
        .transform(lambda carriers: carriers.sortBy(lambda t: t[1], ascending=True)) \
        .pprint(100)

spark_streaming_context.start()
spark_streaming_context.awaitTermination()
