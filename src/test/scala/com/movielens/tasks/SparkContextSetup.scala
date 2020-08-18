package com.movielens.tasks
import org.apache.spark.sql.SparkSession
trait SparkContextSetup {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark test session").getOrCreate()
  }

}