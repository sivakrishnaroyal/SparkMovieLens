package com.movielens.tasks

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Movies {

  def main(args: Array[String]): Unit = {
    // Enabling Logging with Error log and Custom log
    Logger.getLogger("org").setLevel(Level.ERROR)
    val logger = LoggerFactory.getLogger(Ratings.getClass)
    //Declaring arguments for Input and Output Paths
    val inputPath = args(0)
    val outputPath = args(1)
    //Intializing SparkSession to run on local mode with default configurations
    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieGenre")
    val sc = new SparkContext(conf)
    //Load the movie dataset using RDD and splitting by delimiter
    val movies = sc.textFile(inputPath + "movies.dat")
    val moviesPair = movies.map(line => line.split("::")).map(column => (column(1),column(2)))
    val movie_genres = moviesPair.flatMapValues(value => value.split('|'))
    logger.info("Splitting the Movies file  based on the genre completed")
    val genres = movie_genres.map(line => (line._2,line._1))
    val finalMovies = genres.groupByKey.map(movie => (movie._1,movie._2.size)).sortBy(_._1, true)
    logger.info("Grouping by Movie Genre completed")
    try {
      logger.info("Saving the file to  project target directory")
      //Writing the output data to local
      val movieCsv = finalMovies.coalesce(1).map(x => x.productIterator.mkString(",")).saveAsTextFile(outputPath + "MovieGenres")
    } catch {
      case oe:Exception => logger.warn("Unable to write the file as directory already exists."  +oe.getMessage)
    }
    //Renaming the Part file in the target directory based on OS requirement
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if(File.separator == '\\') {
      val filePath = outputPath + "MovieGenres" + '\\'
      fs.rename(new Path(filePath + "part-00000"), new Path(filePath + "MovieGenreCount.csv"))
      logger.info("File Rename in target directory completed")
    }  else {
      val filePath = outputPath + "MovieGenres" + '/'
      fs.rename(new Path(filePath + "part-00000"), new Path(filePath + "MovieGenreCount.csv"))
      logger.info("File Rename in target directory completed")
    }
    sc.stop()
  }

}
