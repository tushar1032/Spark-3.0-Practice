package udemy

import udemy.Setup.setup

object FriendsByAge {

  def main(args: Array[String]) {

   val sc = setup("FriendsByAge")

    val input = sc.textFile("./src/resources/fakefriends.csv")
    val processedInput = input.map(parseLine)
    val result = processedInput.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2).collect()
    result.sorted.foreach(println)
  }

  def parseLine(line: String): (Int, Int) = {
    val words = line.split(",")
    val age = words(2).toInt
    val numOfFriends = words(3).toInt
    (age, numOfFriends)
  }

}
