from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.functions import col
from datetime import datetime
import pyspark.sql.functions as sf
import sys

#Parse line of file
def readInputFile(line):
    data = line.split(",")
    return Row(itemId=int(data[1]), rating=int(data[2]))
	
if __name__ == "__main__":
    startTime = datetime.now()

    #Create Spark Context 
    sc = SparkSession.builder.appName("A2SparkSQL").getOrCreate()
    
    #Reduce amount of console logs output
    sc.sparkContext.setLogLevel('WARN')

    #Get our file from HDFS
    fileLines = sc.sparkContext.textFile("hdfs://localhost/user/hadoop/ratings/ratings.dat")

    #Read our file line by line and map it
    movieRatings = fileLines.map(readInputFile)

    #Create data frame for entries
    moviesDataFrame = sc.createDataFrame(movieRatings)

    #Count amount of reviews for each movie ID
    reviewCounts = moviesDataFrame.groupBy("itemId").count()

    #Use aggregate function to  find average rating value (stored in 'average') within each grouping of movie IDs 
    avgRatings = moviesDataFrame.groupBy("itemId").agg(sf.avg('rating').alias('average'))

    #Create our table, and then sort it by the average column created prior
    avg_counts = reviewCounts.join(avgRatings, 'itemId').sort(col("average").asc())

    #Finally, display it
    avg_counts.show()
	
    endTime = datetime.now()
    totalTime = endTime - startTime
    print("Time (seconds): "+ str(totalTime.seconds))

    #End session as it is no longer needed
    sc.stop()