package udemy

import udemy.Setup.setup

import scala.io.{Codec, Source}

object MostPopularMarvelHero {

  def main(args: Array[String]) = {
    val sc = setup("MostPopularSuperhero")
    val superHeroIdVsName = loadSuperHeroNames
    val input = sc.textFile("./src/resources/Marvel-graph.txt")
    val result = input.map(line => (line.split(" ")(0).toInt,line.split(" ").tail))
      .mapValues(list => list.size)
      .reduceByKey((x,y) => x + y)
      .map(x => (x._2,x._1))
      .max()
      val name = superHeroIdVsName(result._2)
      val count = result._1
    println(s"$name : $count")

  }

  def loadSuperHeroNames(): Map[Int, String] = {
    implicit val codec = Codec("ISO-8859-1")
    var superHeroIdVsName: Map[Int,String] = Map()
    val lines = Source.fromFile("./src/resources/Marvel-names.txt").getLines()
    for(line <- lines) {
      val fields = line.split('\"')
        superHeroIdVsName += (fields(0).trim.toInt -> fields(1))
    }
    superHeroIdVsName
  }

}
