package udemy

import udemy.Setup.setup

import scala.io.{Codec, Source}

object MostHighRatedMovie {

  def main(args: Array[String]) = {
    val sc = setup("MostHighRatedMovie")

    val movieIdVsName = sc.broadcast(loadMovieNames)

    val input = sc.textFile("./src/resources/ml-100k/u.data")
    val processedInput = input.map(parseLine)
    processedInput.mapValues(rating => (rating,1))
                  .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
                  .mapValues(tuple => tuple._1 / tuple._2)
                  .map(x => (x._2,x._1))
                  .sortByKey(ascending = false)
                  .map(x => (movieIdVsName.value(x._2),x._1))
                  .collect()
                  .foreach(println)
  }

  def parseLine(line: String) = {
    val fields = line.split("\t")
    val movieId = fields(1).toInt
    val rating = fields(2).toInt
    (movieId,rating)
  }

  def loadMovieNames() = {
    implicit val codec = Codec("ISO-8859-1")
    var movieIdVsName: Map[Int,String] = Map()
    val lines = Source.fromFile("./src/resources/ml-100k/u.item").getLines()
    for(line <- lines) {
      val fields = line.split('|')
      if(fields.length > 1) {
        movieIdVsName += (fields(0).toInt -> fields(1))
      }
    }
    movieIdVsName
  }

}
