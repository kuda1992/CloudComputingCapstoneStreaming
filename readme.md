# Cloud Computing Capstone Streaming 

The project uses spark and kafka to answer various questions using the us statistics aviation data https://www.transtats.bts.gov/DataIndex.asp


## System Architecture
In this project I have used Kafka, Spark Streaming, and S3.

Kafka is used to get data from s3 and publish it to Kafka topic (aviation dataset). I use an s3 java client to get the data from s3 which downloads it to a remote machine and sends it to kafka. 


 1. Kafka Producer [KafkaProduer](https://github.com/kuda1992/GetS3AviationData/blob/master/src/main/java/KafkaProducerClient.java)
 2. S3 client [Get S3 Data](https://github.com/kuda1992/GetS3AviationData/blob/master/src/main/java/GetS3AviationData.java)

	Send to jar to ec2 instance
	
	```scp -i "newKuda.pem" /Users/kuda/aws/MapReduce/GetS3AviationData/out/artifacts/GetS3AviationData_jar/GetS3AviationData.jar ubuntu@ec2-18-130-83-25.eu-west-2.compute.amazonaws.com:~/```
	
	Run the jar onto the ec2 instance
	
	```java --jar GetS3AviationData.jar 'awskeyid' 'awssecret''```




### Question 1
- Rank the top 10 most popular airports by numbers of flights to/from the airport.

	To answer this question I have used Python Spark Streaming combined with Kafka. Spark streaming application gets data from a Kafka topic (aviation-dataset) which applies backpressure which only takes in 100 records at a time and processes them to get the most popular airport in descending order. 
	
	#### Query

	```
	df = KafkaUtils.createDirectStream(ssc, [source_topic], {"metadata.broker.list": broker}, valueDecoder=decoder)

    df \
        .map(get_original_airport_and_destination_airport) \
        .filter(lambda line: len(line) > 3) \
        .filter(lambda line: Helpers.is_airport(line)) \
        .flatMap(lambda line: line.split(",")) \
        .countByValue() \
        .transform(lambda airports: airports.sortBy(lambda t: t[1], ascending=False)) \
        .foreachRDD(handler)
	
	``` 
	
	#### Running the application
	
	```spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --jars  spark-streaming-kafka-0-8_2.11-2.0.0-preview.jar streaming/MostPopularAirports.py localhost:9092 aviation-dataset```
	
    [MostPopularAirports.py](https://github.com/kuda1992/CloudComputingCapstoneStreaming/blob/master/streaming/MostPopularAirports.py)
    
    #### Results 
    
    ```
	('ATL', 67787)
	('ORD', 59872)
	('DFW', 47735)
	('DEN', 38959)
	('LAX', 37909)
	('PHX', 35388)
	('IAH', 31061)
	('LAS', 30584)
	('DTW', 28754)
	('EWR', 24937)
    ```

### Question 2
- Rank the top 10 airlines by on-time arrival performance.

	This questions asks us to rank the top 10 airlines with the least arrival delay. So we get each airline and the average delay per record. Group each carrier by key which allows us to calculate the average delay for each airline based on the count. We then sort the results in ascending order and print the first 10 records which show the top carriers  on arrival performance. 

	#### Query

	```
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
	``` 
	#### Running the application
	
	```spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --jars  spark-streaming-kafka-0-8_2.11-2.0.0-preview.jar streaming/AverageCarrierOnArrivalDelay.py localhost:9092 aviation-dataset```
	
	[AverageCarrierOnArrivalDelay.py](https://github.com/kuda1992/CloudComputingCapstoneStreaming/blob/master/streaming/AverageCarrierOnArrivalDelay.py)
	
	#### Results
	
	```
	('HA', -2.5192846369316957)
	('KH', -2.5125)
	('US', 1.823384311790824)
	('DL', 4.4382083254442675)
	('B6', 4.858456222619855)
	('WN', 6.868690501163755)
	('F9', 7.122230909564764)
	('FL', 7.2162202233863795)
	('OH', 7.546905195737398)
	```
	
### Question 3
- For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.

	This question allows us to rank the top 10 carriers for each airport. To calculate this we need to submit an airport as a query which will filter the results based on the query. The filtered results will be used to get the carrier and average departure delay as a tuple, which will be grouped by key and then calculate the average,
	
	### Query
	```
	lines \
        .map(get_airport_carrier_and_departure_delay) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: Helpers.is_carrier(line) and Helpers.is_airportid(line)) \
        .filter(lambda line: line.get('airport') == airport_filter) \
        .map(lambda line: (line.get('carrier'), line.get('departure_delay'))) \
        .groupByKey() \
        .map(calculate_average) \
        .transform(lambda carriers: carriers.sortBy(lambda t: t[1], ascending=True)) \
        .pprint(100)
	```
	
	#### Running the application
	
	```spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --jars  spark-streaming-kafka-0-8_2.11-2.0.0-preview.jar streaming/topCarriersOnDepartureForEachAirport.py localhost:9092 aviation-dataset ATL```

	### Results 
	```
	**ATL**
	('NW', 0.0738255033557047)
	('XE', 0.5576923076923077)
	('OO', 7.407494145199063)
	('US', 8.698245614035088)
	('OH', 19.636170212765958)
	('UA', 24.31140350877193)
	```
	
	[topCarriersOnDepartureForEachAirport.py](https://github.com/kuda1992/CloudComputingCapstoneStreaming/blob/master/streaming/topCarriersOnDepartureForEachAirport.py)

### Question 4
- For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X.

	This question allows us to rank the top 10 airports for each airport. To calculate this we need to submit an airport as a query which will filter the results based on the query. The filtered results will be used to get the airport and average departure delay as a tuple, which will be grouped by key and then calculate the average,
	
	### Query
	```
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
	```

	#### Running the application
	
	```spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --jars  spark-streaming-kafka-0-8_2.11-2.0.0-preview.jar streaming/topAirportsOnDepartureForEachAirport.py localhost:9092 aviation-dataset ATL```
	
	#### Results 
	```
	 **ATL**
	('STX', -2.8)
	('TUP', -1.8888888888888888)
	('HDN', 0.5483870967741935)
	('JAC', 0.8333333333333334)
	('ABQ', 1.1149425287356323)
	('BZN', 1.8)
	('BDL', 2.3353293413173652)
	('SNA', 3.5428571428571427)
	('TUS', 4.160714285714286)
	('JAX', 4.251599147121535)
	```
	
	[topAirportsOnDepartureForEachAirport.py](https://github.com/kuda1992/CloudComputingCapstoneStreaming/blob/master/streaming/topAirportsOnDepartureForEachAirport.py)

### Question 5
- For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.

	This question allows us to calculate the the top 10 carriers between to destinations pairs. We submit two destinations as the query to the application. For example ATL -> OLD, we then first filter the results based on the submitted query and get the carrier and departure delay as a tuple. The records are grouped by carrier and the calculate the average arrival delay. The results are then send to a Kafka topic (average-airport-delay-for-each-airport). We have an Kafka Consumer listening to the topic and sending the results to DynamoDB database. 
	
	### Query
	```
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
        .take(10) \
        .foreachRDD(handler)
	```
	
	### Kafka Consumer 
	
	```
		public AverageAirportDelayForEachAirportConsumer(KafkaConsumerClient kafkaConsumerClient, DynamoDBClient dynamoDBClient) {

        KafkaConsumer<Long, String> kafkaConsumer = kafkaConsumerClient.consumer;
        dynamoDB = dynamoDBClient.dynamoDB;

        String topic = "average-airport-delay-for-each-airport";
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        LOGGER.info("Listening to records on topic: " + topic);

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = kafkaConsumer.poll(1000);
            consumerRecords.forEach(this::sendTopicRecordToDynamoDB);
            kafkaConsumer.commitAsync();
        }

    }

    private void sendTopicRecordToDynamoDB(ConsumerRecord<Long, String> consumerRecord) {
        LOGGER.info("Record key: " + consumerRecord.key());
        LOGGER.info("Record value: " + consumerRecord.value());
        LOGGER.info("Record partition: " + consumerRecord.partition());
        LOGGER.info("Record offset: " + consumerRecord.offset());
        
        final String[] carrierAndDelayAverage = consumerRecord.value()
                .replace("(", "")
                .replace("[", "")
                .replace("]", "")
                .replace(")", "")
                .replace("\"", "")
                .replace("'", "")
                .split(",");

        if (carrierAndDelayAverage.length == 2) {
            final String carrier = carrierAndDelayAverage[0];
            final Float averageDelay = Float.parseFloat(carrierAndDelayAverage[1]);
            LOGGER.info("Sending carrier: " + carrier + " with average delay " + averageDelay);

            Table table = dynamoDB.getTable("average-airport-delay-for-each-airport-streaming");

            try {
                final Item item = new Item()
                        .withPrimaryKey("average_delay", averageDelay)
                        .with("carrier", carrier);

                final PutItemOutcome putItemOutcome = table.putItem(item);
                LOGGER.info("Item has been put into database successfully" + putItemOutcome.getPutItemResult());

            } catch (Exception e) {
                LOGGER.error("Failed to put item into table");
                e.printStackTrace();
            }

        }

    }
	```
	
	[AverageAirportDelayForEachAirportConsumer](https://github.com/kuda1992/AverageAirportDelayForEachAirportConsumer/blob/master/src/main/java/AverageAirportDelayForEachAirportConsumer.java)
	
	#### Running the consumer
	
	```java -jar AverageAirportDelayForEachAirportConsumer.jar AKIA5XVWCIBM54RUJ44Q b9kSTvhBiL4rWiTGTI3ZYZXiTf3aIfGhjTN1mPKd localhost:9092 cloud-computing-capstone average-airport-delay-for-each-airport```

	#### Results
	```
	**ATL -> OLD**
	('OO', 9.433333333333334)
	('UA', 28.142857142857142)

	```
	
	
	#### Running the application
	
	```spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --jars  spark-streaming-kafka-0-8_2.11-2.0.0-preview.jar streaming/averageAirportArrivalDelayForEachAirport.py localhost:9092 18.130.83.25:2181 aviation-dataset ATL OLD```
	
	[averageAirportArrivalDelayForEachAirport.py](https://github.com/kuda1992/CloudComputingCapstoneStreaming/blob/master/streaming/averageAirportArrivalDelayForEachAirport.py)

### Question 6
- Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?

	The CCDF of the popularity for airports looks like the following.
	![Power Law Fit](https://github.com/kuda1992/CloudComputingCapstoneStreaming/blob/master/Top10CarriersDepartureDelayForAirport.png)
	
	The CCDF of power-law distributions should be a straight line. Also the lognormal distribution fits
	better to the empirical data. So, the popularity of the airports definitely doesn't follow Zipf distribution.
	
	Loglikelyhood ration tests gives us the following results, when comparing the fitted power-law and lognormal distributions:
	```
	R -3.485522, p 0.000491
	```
	As R is negative, the empirical data is more likely follows a lognormal distribution.

	#### References
	* [Python code in GitHub](https://github.com/kuda1992/CloudComputingCapstoneStreaming/blob/master/streaming/popularity_check.py)
	* [Python powerlaw package usage explanation](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3906378/)

### Question 7
- Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements (for specific queries, see the Task 1 Queries and Task 2 Queries):

	#### Part 1
	The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs on January 5, 2008, Y-Z must depart on January 7, 2008
	
	#### Part 2
	 Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
	 
	 #### Part 3
	 Tom wants to arrive at each destination with as little delay as possible. You can assume you know the actual delay of each flight.