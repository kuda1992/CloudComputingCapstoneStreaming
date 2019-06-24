from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructField

import sys

bestFlightToTakeSchema = StructType([
    StructField('Origin Airport', StringType(), True),
    StructField('Destination Airport', StringType(), True),
    StructField('Date', StringType(), True),
    StructField('AM OR PM', StringType(), True),
    StructField('Carrier', StringType(), True),
    StructField('Flight Number', FloatType(), True),
    StructField('Time', StringType(), True),
    StructField('Average Arrival Delay', FloatType(), True)
])

if __name__ == "__main__":

    if len(sys.argv) < 6:
        print("Not enough arguments")
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Best Flight To Take In 2008") \
        .getOrCreate()

    from_airport = sys.argv[1]
    to_airport = sys.argv[2]
    date = sys.argv[3]
    am_pm = sys.argv[4]

    file = spark.read.text(sys.argv[5])

    remove_quotes = lambda airport_string: airport_string.replace('"', '')


    def is_filtered_airport(airport):
        return airport[0] == from_airport and airport[1] == to_airport


    def is_filtered_date(airport):
        return airport[2] == date and airport[3] == am_pm


    def split_string(first_string, secondstring):
        [from_airport_1, to_airport_1, date_1, am_pm_1] = first_string.split(" ")
        [carrier, flight_number, time, delay] = secondstring.split(" ")
        return [from_airport_1, to_airport_1, date_1, am_pm_1, carrier, float(flight_number), time, float(delay)]


    lines = file.rdd \
        .cache() \
        .keys() \
        .map(lambda l: l.split("\t")) \
        .map(lambda t: [remove_quotes(t[0]), remove_quotes(t[1])]) \
        .map(lambda t: split_string(t[0], t[1])) \
        .filter(lambda t: is_filtered_airport(t)) \
        .filter(lambda t: is_filtered_date(t))

    df = spark \
        .createDataFrame(lines, schema=bestFlightToTakeSchema) \
        .orderBy(["Origin Airport"])

    df.coalesce(1) \
        .write \
        .option("header", "true") \
        .csv(sys.argv[6], header=True)
