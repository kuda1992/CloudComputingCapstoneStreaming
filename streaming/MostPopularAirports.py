from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from streaming.utils.helpers import Helpers

import sys

MostPopularAiportsSchema = StructType([StructField('Airport', StringType(), True), StructField('Count', StringType(), True)])


def get_original_airport_and_destination_airport(line):
    cols = line.split(",")

    if len(cols) < 35:
        return ""
    original_aiport = Helpers.remove_quotes(cols[14])
    destination_aiport = Helpers.remove_quotes(cols[24])
    return original_aiport + "," + destination_aiport


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Not enough arguments")
        exit(-1)

    spark_context = SparkContext(appName="Most Popular Airports")
    spark_context.setLogLevel("WARN")

    spark_streaming_context = StreamingContext(spark_context, 1)

    lines = spark_streaming_context.textFileStream(sys.argv[1])

    lines \
        .map(get_original_airport_and_destination_airport) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: Helpers.is_airport(line)) \
        .flatMap(lambda line: line.split(",")) \
        .countByValue() \
        .transform(lambda airports: airports.sortBy(lambda t: t[1], ascending=False)) \
        .pprint(10)

spark_streaming_context.start()
spark_streaming_context.awaitTermination()
