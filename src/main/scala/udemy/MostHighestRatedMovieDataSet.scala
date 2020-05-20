package udemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.{Codec, Source}

object MostHighestRatedMovieDataSet {

  def loadMovieName() = {
    implicit val codec = Codec("ISO-8859-1")
    var movieIdVsName: Map[Int, String] = Map()
    val lines = Source.fromFile("C:/Users/tusha/IdeaProjects/SparkScalaCourse/src/resources/ml-100k/u.item").getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1)
        movieIdVsName += (fields(0).toInt -> fields(1))
    }
    movieIdVsName
  }


  case class MovieDataSet(id: Int, rating: Double)


  def getMovieIdVsRatingsDataSet(ss: SparkSession) = {
    val input = ss.sparkContext.textFile("C:/Users/tusha/IdeaProjects/SparkScalaCourse/src/resources/ml-100k/u.data")
    import ss.implicits._
    input.map(createRDDForMovieDataSet).toDS()
  }

  def createRDDForMovieDataSet(line: String) = {
    val fields = line.split("\\s+")
    new MovieDataSet(fields(1).toInt, fields(2).toDouble)
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("MostHighestRatedMovieDataSet")
      .getOrCreate()

    val movieIdVsName = ss.sparkContext.broadcast(loadMovieName)

    val movieIdIdVsRatingsDataSet = getMovieIdVsRatingsDataSet(ss).cache()

    val totalViewerPerMovieId = movieIdIdVsRatingsDataSet.groupBy("id").count()
    val totalRatingsPerMovieId = movieIdIdVsRatingsDataSet.groupBy("id").sum("rating")

    import ss.implicits._
    val resultSet = totalRatingsPerMovieId.join(totalViewerPerMovieId, "id")
    resultSet.createOrReplaceTempView("finalTable")
    val finalResult = ss.sql("select id,(`sum(rating)` / count) as avgRating from finalTable").orderBy($"avgRating".desc).limit(5).collect()

    for (result <- finalResult) {
      var highestRatedMovie = movieIdVsName.value(result.getInt(0))
      var highestRating = result.getDouble(1)
      println(s"$highestRatedMovie : $highestRating")
    }
    ss.stop()
  }

}
