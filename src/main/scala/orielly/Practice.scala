package orielly

import java.time.Year

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Practice {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var spark: SparkSession = getSparkSession
    /**
     * Create a Managed Table
     */
    managedTable(spark)

    /**
     * Create an (un) Managed Table
     */
    unManagedTable(spark)

    /**
     * Create Global and Temporary View
     */
    globalAndTemporaryView(spark)

    /**
     * Read a managed Table
     */
    spark = readAnAlreadySavedManagedTable(spark)

    /**
     * Create a UDF
     */
    executeUDF(spark)

    /**
     * Create a DataFrame from a Table in PostgreSQL using JDBC
     */
    readDFFromDatabase(spark)

    spark.close()
  }

  def readDFFromDatabase(spark:SparkSession): Unit = {
    println("************************************READ A DF FROM DATABASE**********************************")
    val df = spark.read.format("jdbc")
      .option("url","jdbc:postgresql://localhost:5432/spark_db")
      .option("user","postgres")
      .option("password","adept")
      .option("dbtable","public.spark_table")
      .load()

    df.show(3);
  }


  val birthYearUDF: Int => Int = (age: Int) => {
    Year.now().getValue - age
  }

  def executeUDF(spark: SparkSession): Unit = {
    println("************************************EXECUTE A UDF*******************************************")
    spark.udf.register("getBirthYear",birthYearUDF)
    val df = getDataFrame(spark)
    df.createOrReplaceTempView("dfTable")
    spark.sql("select id,name,age,totalNumOfFriends,getBirthYear(age) as Year from dfTable").show(5)
    println("************************************EXECUTE A UDF USING DSL*********************************")
    import org.apache.spark.sql.functions.udf
    import spark.implicits._
    val func = udf(birthYearUDF)
    df.select("id","name","age","totalNumOfFriends").withColumn("Year",func($"age")).show(5)

  }


  private def getSparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Practice Examples")
      .getOrCreate()
    spark
  }

  def readAnAlreadySavedManagedTable(spark: SparkSession): SparkSession = {
    println("************************************READ MANAGED TABLE**************************************")
    createAManagedTable(spark)
    println("These are the databases of the old Spark Session")
    spark.catalog.listDatabases().foreach(x => println(x.name))
    spark.close()
    val newSession = getSparkSession
    if(newSession.catalog.databaseExists("TEST")) {
      readAManagedTable(newSession)
    }
    else {
      println("The Old Databases are gone with the Spark Session")
      println("These are the new Databases")
      newSession.catalog.listDatabases().foreach(x => println(x.name))
    }
    newSession
  }

  def globalAndTemporaryView(spark: SparkSession): Unit = {
    println("************************************GLOBAL VIEW*********************************************")
    val df = getDataFrame(spark)
    df.createOrReplaceGlobalTempView("globalView")
    spark.sql("SELECT * FROM global_temp.globalView").show(5)
    spark.sql("DROP VIEW global_temp.globalView")

    println("************************************TEMP VIEW***********************************************")
    df.createOrReplaceTempView("tempView")
    spark.sql("SELECT * FROM tempView").show(5)
    spark.sql("DROP VIEW tempView")
  }

  def managedTable(spark: SparkSession): Unit = {

    println("************************************MANAGED TABLE********************************************")
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
    spark.sql("CREATE DATABASE TEST")
    spark.sql("USE TEST")
    spark.sql("Create table if not exists managed_fake_friends_table (ID INT, NAME STRING, AGE INT, TOTALNUMOFFRIENDS INT) USING PARQUET")
    val df = getDataFrame(spark)
    df.write.mode(SaveMode.Append) saveAsTable ("managed_fake_friends_table")
    spark.sql("select * from managed_fake_friends_table").show(5)
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
  }

  def createAManagedTable(spark:SparkSession): Unit = {
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
    spark.sql("CREATE DATABASE TEST")
    spark.sql("USE TEST")
    spark.sql("Create table if not exists managed_fake_friends_table_2 (ID INT, NAME STRING, AGE INT, TOTALNUMOFFRIENDS INT) USING PARQUET")
    val df = getDataFrame(spark)
    df.write.mode(SaveMode.Append) saveAsTable ("managed_fake_friends_table_2")
  }

  def readAManagedTable(spark: SparkSession): Unit = {
    spark.sql("USE TEST")
    spark.sql("SELECT * FROM managed_fake_friends_table_2").show(5)
    println("Now Dropping the Database>>>>>>>>>>>>>>>>>>>>>>>>")
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
  }

  def getDataFrame(spark: SparkSession): DataFrame = {
    val schemaObject = StructType(
      Array(StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType),
        StructField("totalNumOfFriends", IntegerType)))
    spark.read.schema(schemaObject).csv("C:/Users/tusha/IdeaProjects/SparkScalaCourse/src/resources/fakefriends.csv")
  }

  def unManagedTable(spark: SparkSession): Unit = {
    println("************************************UN-MANAGED TABLE******************************************")
    val df = getDataFrame(spark)
    df.write.mode(SaveMode.Overwrite).format("parquet").save("C:/Users/tusha/IdeaProjects/SparkScalaCourse/src/resources/output/parquet")
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
    spark.sql("CREATE DATABASE TEST")
    spark.sql("USE TEST")
    spark.sql("CREATE TABLE IF NOT EXISTS unmanaged_fake_friends_table (ID INT, NAME STRING, AGE INT, TOTALNUMOFFRIENDS INT) USING PARQUET LOCATION 'C:/Users/tusha/IdeaProjects/SparkScalaCourse/src/resources/output/parquet'")
    spark.sql("select * from unmanaged_fake_friends_table").show(5)
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
  }


}
