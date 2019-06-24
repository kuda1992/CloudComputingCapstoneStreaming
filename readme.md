#Cloud Computing Capstone Streaming 

The project uses spark and kafka to answer various questions using the us statistics aviation data https://www.transtats.bts.gov/DataIndex.asp

### Question 1
- Rank the top 10 most popular airports by numbers of flights to/from the airport.
	
	#### Query

	```
		lines \
			.map(get_original_airport_and_destination_airport) \
			.filter(lambda line: len(line) > 1) \
			.filter(lambda line: Helpers.is_airport(line)) \
			.flatMap(lambda line: line.split(",")) \
			.countByValue() \
			.transform(lambda airports: airports.sortBy(lambda t: t[1], ascending=False)) \
			.pprint(10)
	
	``` 
	
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

	#### Query

	```
		lines \
			.map(get_carrier_and_arrival_delay) \
			.filter(lambda line: len(line) > 1) \
			.filter(lambda line: Helpers.is_carrier(line)) \
			.map(lambda airport: (airport.get('carrier'), airport.get('average_delay'))) \
			.groupByKey() \
			.map(calculate_average) \
			.transform(lambda carriers: carriers.sortBy(lambda t: t[1], ascending=True)) \
			.pprint(100)
	
	``` 
	
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
	('AS', 8.177964032874929)
	('CO', 9.002926320853042)
	('EV', 9.17060833902939)
	('NW', 9.394402118824184)
	('XE', 9.886929702048418)
	('9E', 12.267573066962633)
	('YV', 14.497241846511178)
	('AA', 15.071998420221169)
	('MQ', 15.115856940163413)
	('OO', 18.224468614180378)
	('UA', 19.83305925560489)
	```
	
### Question 3
- For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.
	
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

### Question 4
- For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X.

### Question 5
- For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.

### Question 6
- Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?

### Question 7
- Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements (for specific queries, see the Task 1 Queries and Task 2 Queries):

	#### Part 1
	The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs on January 5, 2008, Y-Z must depart on January 7, 2008
	
	#### Part 2
	 Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
	 
	 #### Part 3
	 Tom wants to arrive at each destination with as little delay as possible. You can assume you know the actual delay of each flight.