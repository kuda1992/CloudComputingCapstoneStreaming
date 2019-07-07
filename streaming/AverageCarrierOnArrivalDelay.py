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


def get_carrier_and_arrival_delay(line):
    cols = line[1].split(",")

    if len(cols) < 35:
        return {}
    carrier = remove_quotes(cols[6])
    average_delay = remove_quotes(cols[44])
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

    conf = SparkConf()
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.backpressure.initialRate", "100")

    broker = sys.argv[1]
    kafka_host = sys[2]
    source_topic = sys.argv[3]
    airport_filter = sys.argv[4]

    spark_context = SparkContext(appName="Average Carrier On Arrival Delay", batchSize=50, conf=conf)
    spark_context.setLogLevel("WARN")

    print("The broker is " + broker)
    print("The source topic is " + source_topic)

    spark_streaming_context = StreamingContext(spark_context, 60)
    df = KafkaUtils.createDirectStream(spark_context, [source_topic], {"metadata.broker.list": broker})

    df \
        .map(get_carrier_and_arrival_delay) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: is_carrier(line)) \
        .map(lambda airport: (airport.get('carrier'), airport.get('average_delay'))) \
        .groupByKey() \
        .map(calculate_average) \
        .transform(lambda carriers: carriers.sortBy(lambda t: t[1], ascending=True)) \
        .foreachRDD(handler)

    spark_streaming_context.start()
    spark_streaming_context.awaitTermination()
