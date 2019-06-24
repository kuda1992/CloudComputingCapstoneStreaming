#Cloud Computing Capstone Streaming 

The project uses spark and kafka to answer various questions using the us statistics aviation data https://www.transtats.bts.gov/DataIndex.asp

###Question 1
- Rank the top 10 most popular airports by numbers of flights to/from the airport.

`lines \
        .map(get_original_airport_and_destination_airport) \
        .filter(lambda line: len(line) > 1) \
        .filter(lambda line: Helpers.is_airport(line)) \
        .flatMap(lambda line: line.split(",")) \
        .countByValue() \
        .transform(lambda airports: airports.sortBy(lambda t: t[1], ascending=False)) \
        .pprint(10)`


###Question 2
- Rank the top 10 airlines by on-time arrival performance.

###Question 3
- For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.

###Question 4
- For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X.

###Question 5
- For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.

###Question 6
- Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?

###Question 7
- Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements (for specific queries, see the Task 1 Queries and Task 2 Queries):

	#### Part 1
	The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs on January 5, 2008, Y-Z must depart on January 7, 2008
	
	#### Part 2
	 Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
	 
	 #### Part 3
	 Tom wants to arrive at each destination with as little delay as possible. You can assume you know the actual delay of each flight.