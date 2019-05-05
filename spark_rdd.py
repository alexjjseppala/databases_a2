from pyspark import SparkConf, SparkContext
from datetime import datetime
import sys
 
#Returns line as key with value tuple 
def readInputFile(line):
    data = line.split(",")
    return (int(data[1]), (int(data[2]), 1))

#Returns key value tuple in form (key,(average rating, number of ratings))
def getAverage(line):
    return (line[0], (float(line[1][0])/float(line[1][1]),line[1][1]))

if __name__ == "__main__":
    startTime = datetime.now()
    
    #Create Spark Context
    conf = SparkConf().setAppName("A2SparkRDD")
    sc = SparkContext(conf = conf)
 
    #Get our file from HDFS
    ratingsFile = sc.textFile("hdfs://localhost/ratings.data")

    #Read our file line by line and map it
    ratings = ratingsFile.map(readInputFile)

    #Reduce with a lambda function to get the total number of reviews and sum of ratings
    sumValues = ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    #Another mapping to get average rating
    unsortedResults = sumValues.map(getAverage)
	
    #Sort in descending order
    sortedResults = unsortedResults.sortBy(lambda row: row[1][0])

    #Take only the first 20
    finalResults = sortedResults.take(20)

    endTime = datetime.now()
    totalTime = endTime - startTime
    print("Final Results")
    print("Time (seconds): "+ str(totalTime.seconds))
    print("Movie ID  Average Rating  No. of Ratings:")
    for result in finalResults:
        print(str(result[0])+"\t"+str(result[1][0])+"\t"+str(result[1][1]))