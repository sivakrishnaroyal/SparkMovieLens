package com.movielens.tasks

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

object TopMovies {
    def main(args: Array[String]): Unit = {
      // Enabling Logging with Error log and Custom log
      Logger.getLogger("org").setLevel(Level.ERROR)
      val logger = LoggerFactory.getLogger(Ratings.getClass)
      //Declaring arguments for Input and Output Paths
      val inputPath = args(0)
      val outputPath = args(1)

      //Intializing SparkSession to run on local mode with default configurations
      val spark = SparkSession.builder.appName("Movie Ranks").master("local[*]").getOrCreate()

      import spark.implicits._
      //Split the movie data set and loading into DataFrame
      val movies = spark.read.textFile(inputPath + "movies.dat")
      val movie_df = movies.map(line => line.split("::")).map{line =>
        val MovieID = line(0).toInt
        val Title = line(1)
        (MovieID, Title)
      }.toDF("MovieID", "Title")

      logger.info("Movie data into DataFrame Completed")

      //Split the Rating data and apply transformations to get the average rating and loading into DataFrame
      val ratings = spark.read.textFile(inputPath + "ratings.dat")
      val ratings_df = ratings.map { line =>
        val UserID = line.split("::")(0).toInt
        val MovieID = line.split("::")(1).toInt
        val Rating = line.split("::")(2).toFloat
        (UserID,MovieID,Rating)
      }.toDF("UserID", "MovieID", "Rating").
        groupBy("MovieID").
        agg(avg("Rating").as("AverageRating"),count("UserID").as("MaxUsers"))

      logger.info("Average rating data load into DataFrame Completed")
      //MovieID column renaming in Rating file to perform Join operation
      val averageRating = ratings_df.withColumn("AverageRating", round(col("AverageRating"),3))

      // Applying inner join on Movie and average rating dataframes based MovieID
      val movieRatingJoin = movie_df.join(averageRating,  Seq("MovieID"), "inner").persist()
      logger.info("Join operation completed and persisted the Join dataframe to Memory")

      val topMovies = movieRatingJoin.orderBy(desc("MaxUsers"))
      //Generating the Index number to provide the Rank
      val movieRank = spark.createDataFrame(
        topMovies.rdd.zipWithIndex.map {
          case (row, index) => Row.fromSeq(row.toSeq :+ index + 1)
        },StructType(topMovies.schema.fields :+ StructField("Rank", LongType, false)))
//Writing data into Parquet file with top 100 popular movies
       movieRank.select("Rank","MovieID","Title","AverageRating").
         limit(100).
         coalesce(1).
         write.
         format("parquet").
         mode("overWrite").save(outputPath + "popularMovies")
      // Unpersist the join dataset
      movieRatingJoin.unpersist()
        spark.stop()
    }
}
