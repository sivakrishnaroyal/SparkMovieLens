package com.movielens.tasks

import org.scalatest.funsuite.AnyFunSuite

// Test cases performed using ScalaTest FunSuite

class RatingDataSpec extends AnyFunSuite with SparkContextSetup {

  val sc = spark.sparkContext
  val ratings = sc.textFile("src/test/Resources/ratings.dat")
  //Test Scenario - 1
  test("Rating file count") {
    val testCount = ratings.count()
    assert(testCount == 269)
  }
  val ratingsSplit = ratings.map(line => line.split("::")).map { line =>
    val MovieID = line(1).toInt
    val Rating = line(2).toFloat
    (MovieID, (Rating, 1))
  }
  //Test Scenario - 2
  test("Checking the rating field in pair RDD tuple for Float Type") {
    val testType = ratingsSplit.map(x => x._2._1).first
    assert(testType == Int)
  }

  val agg_ratings = ratingsSplit.reduceByKey((acc, value) => (acc._1 + value._1, acc._2 + value._2))
  val avg_ratings = agg_ratings.map(line => (line._1, line._2._2, line._2._1 / line._2._2))

  //Test Scenario -3
  test("Validating the sample MovieID total views and average Rating") {
    val totalRatings = agg_ratings.map(x => x._2._1).take(1)
    val testAverage = BigDecimal(totalRatings(0) / 269).setScale(3, BigDecimal.RoundingMode.HALF_UP)
    assert(testAverage == 3.346)
  }

}