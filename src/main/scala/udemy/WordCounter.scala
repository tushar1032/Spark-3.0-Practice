package udemy

import udemy.Setup.setup

object WordCounter {

  def main(args: Array[String]) = {
    val sc = setup("WordCounter")

    val input = sc.textFile("./src/resources/book.txt")
    val result = input.flatMap(line => line.split("\\W+"))
      .map(words => words.toLowerCase)
      .map(word => (word,1))
      .reduceByKey((x,y) => x + y)
      .map(tuple => (tuple._2,tuple._1))
      .sortByKey(ascending = false)
      .collect()
    result.foreach(println)
  }

}
