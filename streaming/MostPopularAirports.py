from pyspark import SparkConf
from kafka import KafkaProducer
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
import sys
import json


class Helpers:

    def __init__(self):
        print("helpers class")

    @staticmethod
    def remove_quotes(airport_string):
        return airport_string.replace('"', '')

    @staticmethod
    def is_airport(airport_string):
        cols = airport_string.split(",")
        original_aiport = cols[0]
        destination_aiport = cols[1]
        return len(original_aiport) == 3 and len(destination_aiport) == 3

    @staticmethod
    def is_carrier(airport):
        return len(airport.get('carrier')) == 2

    @staticmethod
    def is_airportid(airport):
        return len(airport.get('airport')) == 3


MostPopularAiportsSchema = StructType([StructField('Airport', StringType(), True), StructField('Count', StringType(), True)])


def decoder(s):
    if s is None:
        return None

    # loaded_json = json.loads(s.decode('utf-8'))

    print("******************Decoding*******************")
    print(s)
    print("******************End*******************")
    return s


def handler(messages):
    records = messages.collect()
    producer = connect_kafka_producer()

    if len(records):
        print("sending messages to kafka")
        for message in messages.collect():
            print(message)
            publish_message(producer, "mostpopularairports", message)


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(ex)
    finally:
        return _producer


def publish_message(producer_instance, topic_name, value):
    producer_instance.send(topic_name, json.dumps(value).encode('utf-8'))
    producer_instance.flush()


def get_original_airport_and_destination_airport(line):
    cols = line[1].split(",")
    if len(cols) < 35:
        return ""
    original_aiport = Helpers.remove_quotes(cols[14])
    destination_aiport = Helpers.remove_quotes(cols[24])
    return original_aiport + "," + destination_aiport


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Not enough arguments")
        exit(-1)

    conf = SparkConf()
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.backpressure.initialRate", "100")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    sc = SparkContext(appName="Most Popular Airports", batchSize=50, conf=conf)
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 60)

    broker = sys.argv[1]
    source_topic = sys.argv[2]
    # destionation_topic = sys.argv[3]

    print("The broker is " + broker)
    print("The source topic is " + source_topic)

    df = KafkaUtils.createDirectStream(ssc, [source_topic], {"metadata.broker.list": broker}, valueDecoder=decoder)

    df \
        .map(get_original_airport_and_destination_airport) \
        .filter(lambda line: len(line) > 3) \
        .filter(lambda line: Helpers.is_airport(line)) \
        .flatMap(lambda line: line.split(",")) \
        .countByValue() \
        .transform(lambda airports: airports.sortBy(lambda t: t[1], ascending=False)) \
        .foreachRDD(handler)

    ssc.start()
    ssc.awaitTermination()
