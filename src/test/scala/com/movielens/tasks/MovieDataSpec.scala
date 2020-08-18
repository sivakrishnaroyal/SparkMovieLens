package com.movielens.tasks

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
// Test cases performed using ScalaTest WordSpec

class MovieDataSpec extends AnyWordSpec with Matchers with SparkContextSetup {
  val sc = spark.sparkContext
//Test Case -1
  "should test spark session" in {
    val testSession = spark.range(3).count()
    testSession shouldBe 3
  }
//Test Case -2
  "should test raw file count" in {
    val movies = sc.textFile("src/test/Resources/movies.dat")
    val moviesPair = movies.map(line => line.split("::")).map(column => (column(1), column(2)))
    val movie_genres = moviesPair.flatMapValues(value => value.split('|'))
    val genres = movie_genres.map(line => (line._2, line._1))
    val sample = genres.count()
    sample should equal(22)
  }
//Test Case -3
    "should test any one genre count" in {
      val movies = sc.textFile("src/test/Resources/movies.dat")
      val moviesPair = movies.map(line => line.split("::")).map(column => (column(1), column(2)))
      val movie_genres = moviesPair.flatMapValues(value => value.split('|'))
      val genres = movie_genres.map(line => (line._2, line._1))
      val finalMovies = genres.groupByKey.map(movie => (movie._1, movie._2.size)).sortBy(_._1, true)
      val testGenre = finalMovies.lookup("Action")
      testGenre contains Seq(3)
  }

  }
