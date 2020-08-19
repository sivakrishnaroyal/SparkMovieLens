# SparkMovieLens
MovieLens datasets processing using spark-scala Core and SQL APIs

MovieLens 1 Million Dataset for this exercise. It can be downloaded from http://grouplens.org/datasets/movielens/.
Dataset is available under http://files.grouplens.org/datasets/movielens/ml-1m.zip. 
Please refer to the Read Me file at http://files.grouplens.org/datasets/movielens/ml-1m-README.txt for details about the data.

In this project generated the SPARK-SCALA code for the below activities.
Using Spark-Core API(RDD):
1. To generate the CSV file, it contains list of movies with no of users who rated the movie and average rating per movie and the output file contains 3 columns,
ie: MovieId, No of Users, Average Rating (without header)

2. To generate CSV file, it contains list of unique Genres and no of movies under each genres and the output CSV file contains 2 columns, 
ie: Genres, No of Movies (without header)

Using Spark-Sql API:

To Generate a parquet file that contain the top 100 movies based on their ratings. the output file contains fields, 
ie: Rank (1-100), Movie Id, Title, Average Rating. Rank 1 is the most popular movie. 

Unit Test Cases:

Unit test cases are implemented using ScalaTest for Write unit tests for 1 & 2( WordSpec and FunSpec is used)

Note: All the code written to run on Local mode.

Dependencies:
//java version "1.8.0_261"
//Spark Version "2.4.6" 

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"

// https://mvnrepository.com/artifact/junit/junit
libraryDependencies += "junit" % "junit" % "4.12" % Test

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test
