package udemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Setup {

  def setup(appName: String): SparkContext = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName(appName)
    val sc = new SparkContext(conf)
    sc
  }

}
