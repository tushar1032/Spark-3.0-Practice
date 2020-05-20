package udemy

import udemy.Setup.setup

object RatingsCounter {

  def main(args: Array[String]) {

    val sc = setup("RatingsCounter")
    val lines = sc.textFile("./src/resources/ml-100k/u.data")
    val ratings = lines.map(x => x.toString.split("\t")(2))
    val results = ratings.map(rating => (rating, 1)).reduceByKey((x, y) => x + y).collect()
    val sortedResults = results.toSeq.sortBy(_._2)
    sortedResults.foreach(println)
  }
}
