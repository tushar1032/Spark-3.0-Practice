package udemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TotalAmountSpentByCustomer {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("TotalAmountSpentByCustomer")
    val sc = new SparkContext(conf)
    val input = sc.textFile("C:/Users/tusha/IdeaProjects/SparkScalaCourse/src/resources/customer-orders.csv")
    val processedInput = input.map(parseLine)
    processedInput.reduceByKey((x,y) => x + y).collect().toSeq.sortBy(_._2).foreach(println)
  }

  def parseLine(line: String) = {
    val fields = line.split(",")
    val customerId = fields(0)
    val itemPrice = fields(2).toFloat
    (customerId,itemPrice)
  }



}
