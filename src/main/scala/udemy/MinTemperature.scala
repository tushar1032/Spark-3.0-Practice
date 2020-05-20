package udemy

import udemy.Setup.setup

import scala.math.min

object MinTemperature {

  def main(args: Array[String]): Unit = {

    val sc = setup("MinimumTemperature")

    val input = sc.textFile("./src/resources/1800.csv")
    val processedInput = input.map(parseLine).filter(x => x._2.equals("TMIN"))
    val result = processedInput.map(x => (x._1, x._3)).reduceByKey((x, y) => min(x, y)).collect()
    result.sorted.foreach(println)

  }

  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat
    (stationId, entryType, temperature)
  }

}
