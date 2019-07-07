from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer

import sys
import json


def is_airportid(airport):
    return len(airport.get('airport')) == 3


def is_carrier(airport):
    return len(airport.get('carrier')) == 2


def is_airport(airport_string):
    cols = airport_string.split(",")
    original_aiport = cols[0]
    destination_aiport = cols[1]
    return len(original_aiport) == 3 and len(destination_aiport) == 3


def remove_quotes(airport_string):
    return airport_string.replace('"', '')


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


def is_not_first_line(line):
    cols = line.split(",")

    if remove_quotes(cols[0]) == "YEAR":
        return False
    else:
        return True


def get_flights_details(line):
    cols = line.split(",")

    if len(cols) < 35:
        return {}
    carrier = remove_quotes(cols[6])
    airport = remove_quotes(cols[14])
    dest_airport = remove_quotes(cols[24])
    flight_number = remove_quotes(cols[10])
    departure_date = remove_quotes(cols[5])
    departure_time = remove_quotes(cols[32])
    arrival_delay = remove_quotes(cols[44])

    if len(arrival_delay) < 1:
        arrival_delay = 0
    else:
        arrival_delay = float(arrival_delay)

    return {
        "airport": airport,
        "flight_number": flight_number,
        "stop_over": dest_airport,
        "departure_date": departure_date,
        "departure_time": departure_time,
        "dest_airport": dest_airport,
        "carrier": carrier,
        "arrival_delay": arrival_delay
    }


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


def date_to_data_frame_format(date_string):
    [day, month, year] = date_string.split("/")
    return year + "-" + month + "-" + day


def get_next_date(date_string):
    [day, month, year] = date_string.split("/")

    day = int(day) + 1
    month = int(month)
    year = int(year)

    if day < 10:
        day = '0' + str(day)

    if month < 10:
        month = '0' + str(month)

    return str(year) + "-" + str(month) + "-" + day


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

    origin_airport_filter = sys.argv[4]
    stop_over_airport_filter = sys.argv[5]
    dest_airport_filter = sys.argv[6]
    date_filter = date_to_data_frame_format(sys.argv[7])
    next_date_filter = get_next_date(sys.argv[8])

    print("origin airport " + origin_airport_filter)
    print("stop over airport " + stop_over_airport_filter)
    print("final destionation airport " + dest_airport_filter)
    print("travel date " + date_filter)
    print("next travel date " + next_date_filter)

    spark_context = SparkContext(appName="Best Flight On A Given Date  ", batchSize=50, conf=conf)
    spark_context.setLogLevel("WARN")

    print("The broker is " + broker)
    print("The source topic is " + source_topic)

    spark_streaming_context = StreamingContext(spark_context, 60)

    df = KafkaUtils.createDirectStream(spark_context, [source_topic], {"metadata.broker.list": broker})

    first_flight = df \
        .filter(lambda l: is_not_first_line(l)) \
        .map(get_flights_details) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: is_carrier(line) and is_airportid(line)) \
        .filter(lambda line: line.get('airport') == origin_airport_filter and line.get('stop_over') == stop_over_airport_filter and line.get('departure_date') == date_filter) \
        .map(lambda line: (line.get('airport'), line.get('stop_over'), line.get('departure_date'), line.get('departure_time'), line.get('carrier'), line.get('arrival_delay'))) \
        .transform(lambda carriers: carriers.sortBy(lambda t: t[5], ascending=True))

    second_flight = df \
        .filter(lambda l: is_not_first_line(l)) \
        .map(get_flights_details) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: is_carrier(line) and is_airportid(line)) \
        .filter(lambda line: line.get('airport') == stop_over_airport_filter and line.get('dest_airport') == dest_airport_filter and line.get('departure_date') == next_date_filter) \
        .map(lambda line: (line.get('airport'), line.get('dest_airport'), line.get('departure_date'), line.get('departure_time'), line.get('carrier'), line.get('arrival_delay'))) \
        .transform(lambda carriers: carriers.sortBy(lambda t: t[5], ascending=True))

    first_flight.foreachRDD(handler)
    second_flight.foreachRDD(handler)

    spark_streaming_context.start()
    spark_streaming_context.awaitTermination()
