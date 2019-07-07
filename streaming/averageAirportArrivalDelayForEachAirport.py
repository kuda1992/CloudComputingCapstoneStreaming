from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer

import sys
import json


def remove_quotes(airport_string):
    return airport_string.replace('"', '')


def is_airportid(airport):
    return len(airport.get('airport')) == 3


def is_carrier(airport):
    return len(airport.get('carrier')) == 2


def is_airport(airport_string):
    cols = airport_string.split(",")
    original_aiport = cols[0]
    destination_aiport = cols[1]
    return len(original_aiport) == 3 and len(destination_aiport) == 3


def handler(messages):
    records = messages.collect()
    producer = connect_kafka_producer()

    if len(records):
        print("sending messages to kafka")
        for message in messages.collect():
            print(message)
            publish_message(producer, "top-airports-for-each-airport", message)


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=[kafka_host], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(ex)
    finally:
        return _producer


def publish_message(producer_instance, topic_name, value):
    producer_instance.send(topic_name, json.dumps(value).encode('utf-8'))
    producer_instance.flush()


def get_airport_carrier_and_departure_delay(line):
    cols = line[1].split(",")

    if len(cols) < 35:
        return {}
    carrier = remove_quotes(cols[6])
    dest_airport = remove_quotes(cols[24])
    airport = remove_quotes(cols[14])
    departure_delay = remove_quotes(cols[33])
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

    if len(sys.argv) < 3:
        print("Not enough arguments")
        exit(-1)

    conf = SparkConf()
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.backpressure.initialRate", "100")

    broker = sys.argv[1]
    kafka_host = sys[2]
    source_topic = sys.argv[3]
    origin_airport_filter = sys.argv[4]
    dest_airport_filter = sys.argv[5]

    spark_context = SparkContext(appName="Top 10 Airports Per Destination Pair")
    spark_context.setLogLevel("WARN")

    spark_streaming_context = StreamingContext(spark_context, 1)

    df = KafkaUtils.createDirectStream(spark_streaming_context, [source_topic], {"metadata.broker.list": broker})

    df \
        .map(get_airport_carrier_and_departure_delay) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: is_carrier(line) and is_airportid(line)) \
        .filter(lambda line: line.get('airport') == origin_airport_filter and line.get('dest_airport') == dest_airport_filter) \
        .map(lambda line: (line.get('carrier'), line.get('departure_delay'))) \
        .groupByKey() \
        .map(calculate_average) \
        .transform(lambda carriers: carriers.sortBy(lambda t: t[1], ascending=True)) \
        .foreachRDD(handler)

spark_streaming_context.start()
spark_streaming_context.awaitTermination()
