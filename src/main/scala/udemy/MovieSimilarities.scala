package udemy

import java.nio.charset.CodingErrorAction

import udemy.Setup.setup

import scala.io.{Codec, Source}
import scala.math.sqrt

object MovieSimilarities {

  def loadMoviesNames(): Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val data = Source.fromFile("./src/resources/ml-100k/u.item").getLines()
    var movieNameDict: Map[Int, String] = Map()
    for (line <- data) {
      val fields = line.split('|')
      movieNameDict += (fields(0).toInt -> fields(1))
    }
    movieNameDict
  }

  type MovieRatings = (Int, Double)
  type UserIdVsMovieRatingsPair = (Int, (MovieRatings, MovieRatings))

  def filterDuplicatePairs(userIdVsMovieRatingsPair: UserIdVsMovieRatingsPair) = {
    val firstMovieRating = userIdVsMovieRatingsPair._2._1
    val secondMovieRating = userIdVsMovieRatingsPair._2._2
    firstMovieRating._1 < secondMovieRating._1
  }

  def makePairs(userIdVsMovieRatingsPair: UserIdVsMovieRatingsPair) = {
    val firstMovieRating = userIdVsMovieRatingsPair._2._1
    val secondMovieRating = userIdVsMovieRatingsPair._2._2

    val movie1 = firstMovieRating._1
    val movie2 = secondMovieRating._1

    val rating1 = firstMovieRating._2
    val rating2 = secondMovieRating._2

    ((movie1, movie2), (rating1, rating2))
  }

  type RatingPair = (Double,Double)
  type RatingPairs = Iterable[RatingPair]
  def computeCosineSimilarities(ratingPairs: RatingPairs): (Double, Int) = {

    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    (score, numPairs)
  }

  def main(args: Array[String]): Unit = {
    val sc = setup("MovieSimilarities")
    val movieNameDict = loadMoviesNames()

    val data = sc.textFile("./src/resources/ml-100k/u.data")
    val processedData = data.map(line => line.split("\t")).map(fields => (fields(0).toInt, (fields(1).toInt, fields(2).toDouble)))
    val movieVsRatingPairs = processedData.join(processedData).filter(filterDuplicatePairs)
    val moviePairs = movieVsRatingPairs.map(makePairs).groupByKey()
    val moviePairSim = moviePairs.mapValues(computeCosineSimilarities).persist()

    val inputMovieId = args(0).toInt

    val filteredResult = moviePairSim.filter( x => {
      val coOccurrence = 50
      val covariance = 0.97
      val moviePairs = x._1
      val sim = x._2
      (inputMovieId == moviePairs._1 || inputMovieId == moviePairs._2) && (sim._1 > covariance && sim._2 > coOccurrence)
    })

    val topTenRecommendations = filteredResult.map(x => (x._2,x._1)).sortByKey(ascending = false).take(10);

    for(moviePair <- topTenRecommendations) {
      val similarities = moviePair._1._1
      val numOfOccurrence = moviePair._1._2
      val movies = moviePair._2
      var selectedMovie = movieNameDict(movies._1)
      if(movies._1 == inputMovieId) {
        selectedMovie = movieNameDict(movies._2)
      }

      println(s"Movie name: $selectedMovie")
      println(s"Movie Similarity: $similarities Co-Occurrence: $numOfOccurrence")
      println()
    }
  }

}
