from mrjob.job import MRJob 
from mrjob.step import MRStep
from datetime import datetime
import sys


class MRRatingData(MRJob):

    # SORT_VALUES = True

    #two mapreduce steps, once for calculating the averages and counts, the other for rearranging and sorting
    def steps(self):
           return [ 
                MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_avg_counts)
                ,MRStep(mapper=self.mapper_sort, reducer=self.reducer_sort)
                  ]

    #mapper in the first mapreduce that parses the neccessary data from the dat file, 
    def mapper_get_ratings(self, _, line):
        (userId, movieId, rating, timestamp) = line.split(',')
        #set the movie id as the key, to group the rating scores
        yield int(movieId), int(rating)

    #reducer in the first mapreduce that calculates the count and average for each movie id 
    def reducer_avg_counts(self, movie_id, ratings):
        #initialize the count for ratings per movie_id
        count = 0
        #initialize the sum of the ratings per movie_id
        sum_ratings = 0
        #iterate through the ratings for the moviee_id and sum th counts and rating scores
        for rating in ratings:
            count += 1
            sum_ratings += rating
        
        #yield the count and calculated average, casting to float
        yield(movie_id, (count, (sum_ratings / float(count))))

    #mapper in the second mapreduce that rearranges the row
    def mapper_sort(self, key, value):
        #move the calculated average to the key of the row for sorting purposes
        yield float(value[1]), (key, int(value[0]))

    #reducer in the second mapreduce, will arrange the rows in sorted order based on the key
    def reducer_sort(self, key, values):
        for value in (values):
            yield key, value


if __name__ == '__main__':
    start_time = datetime.now()
    MRRatingData.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
	#Print the time diff in seconds.
    sys.stderr.write("Total execution time "+str(elapsed_time.seconds)+" Seconds\n")
