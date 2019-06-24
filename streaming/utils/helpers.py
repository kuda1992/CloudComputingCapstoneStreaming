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
    def print_results(rdd):
        """
           Print results to screen.
           """
        print
        "----------------- SNAPSHOT ----------------------"
        for line in rdd.collect():
            print(line)

        print
        "SIZE: %d" % rdd.count()

    @staticmethod
    def save_results(rdd, file_location):
        """
        Save results as a report.
        """
        results = rdd.collect()
        if len(results) == 0:
            return

        file = open(file_location, 'w')

        for item in results:
            file.write("%s -> %s on %s: Flight: %s %s at %s. Arrival Delay: %s\n" %
                       (item[0][0], item[0][1], item[0][2], item[1][0], item[1][1], item[1][2], item[1][3]))
