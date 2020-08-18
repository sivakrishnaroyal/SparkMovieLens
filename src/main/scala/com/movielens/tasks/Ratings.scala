package com.movielens.tasks

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Ratings {
  def main(args: Array[String]): Unit = {
    // Enabling Logging with Error log and Custom log
    Logger.getLogger("org").setLevel(Level.ERROR)
    val logger = LoggerFactory.getLogger(Ratings.getClass)
    //Declaring arguments for Input and Output Paths
    val inputPath = args(0)
    val outputPath = args(1)
    logger.info("Intializing SparkSession to run on local mode with default configurations")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Sample")
    val sc = new SparkContext(conf)
    //Load the Rating using RDD and splitting by delimiter
    val ratingsRDD = sc.textFile(inputPath + "ratings.dat")
    val ratingsSplit = ratingsRDD.map(line => line.split("::")).map { line =>
    val MovieID = line(1).toInt
    val Rating = line(2).toFloat
    (MovieID, (Rating, 1))
    }
    logger.info("Aggregation of average ratings started from the Pair RDD of MovieID and Rating attributes")
  try {
    val agg_ratings = ratingsSplit.reduceByKey((acc, value) => (acc._1 + value._1, acc._2 + value._2))
    val avg_ratings = agg_ratings.map(line => (line._1, line._2._2, line._2._1 / line._2._2))
    val final_ratings = avg_ratings.map(x => (x._1, x._2, BigDecimal(x._3).setScale(3, BigDecimal.RoundingMode.HALF_UP))).sortBy(_._2, false)
    logger.info("Saving the final average ratings to destination path as a CSV file")
    //Writing the data to local with CSV file
    final_ratings.coalesce(1).map(x => x._1 + "," + x._2 + "," + x._3).saveAsTextFile(outputPath + "Ratings")
  } catch {
    case oe:Exception => logger.warn("Unable to write the file as directory already exists"  +oe.getMessage)
  }
    //Renaming the target file
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if(File.separator == '\\') {
      val filePath = outputPath + "Ratings" + '\\'
      logger.info("Renaming the part file to target file name")
      fs.rename(new Path(filePath + "part-00000"), new Path(filePath + "AverageRatings.csv"))
      logger.info("Average ratings spark job completed Successfully")
    } else {
      val filePath = outputPath + "Ratings" + '/'
      logger.info("Renaming the part file to target file name")
      fs.rename(new Path(filePath + "part-00000"), new Path(filePath + "AverageRatings.csv"))
      logger.info("Average ratings spark job completed Successfully")
    }
    sc.stop()
  }
}
