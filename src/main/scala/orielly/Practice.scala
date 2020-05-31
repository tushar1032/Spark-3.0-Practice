package orielly

import java.time.Year
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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

    /**
     * Write the data of a Dataframe to a PostgreSQL Table.
     */
    writeToADBTable(spark)

    /**
     * Sql Stat Functions
     */
    statFunctions(spark)

    /**
     * String Manipulation Functions
     */
    stringManipulations(spark)

    /**
     * Handling Date and Time
     */
    handlingDateAndTime(spark)

    /**
     * Usinng Aggregate functions
     */
    usingAggregateFunctions(spark)

    /**
     * Windowing Functions
     */
    handleWindowingFunctions(spark)

    spark.close()
  }

  def handleWindowingFunctions(spark: SparkSession): Unit = {
    val df = getStockMarketDF(spark)

    println("Num of partitions...........")
    println(df.rdd.getNumPartitions)

    println("Good place to use repartition to increase the num of partitions to increase parallelism")
    import spark.implicits._
    val repartitionedDf = df.repartitionByRange(48, $"date")

    println("Data.....")
    repartitionedDf.show(10)

    println("Now the windowing function (lag) with sql....")
    repartitionedDf.createOrReplaceTempView("appleData")
    spark.sql("select *, lag(close,1) over (order by date) as yesterday_close from appleData").show(10)

    println("Complicated Query...............")
    repartitionedDf.withColumn("yesterday_close", expr("lag(close,1) over (order by date)")).select(expr("*"), expr("close - yesterday_close").alias("close_diff")).show(10)

    println("Using Window Spec..........")
    val windowSpec = Window.orderBy($"date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val rank = dense_rank().over(windowSpec)
    df.select(expr("*"), rank).show(10)


  }


  def getStockMarketDF(spark: SparkSession): DataFrame = {
    val schema = StructType(Array(StructField("Date", DateType, false),
      StructField("Open", DoubleType, false),
      StructField("High", DoubleType, false),
      StructField("Low", DoubleType, false),
      StructField("Close", DoubleType, false),
      StructField("Adj_close", DoubleType, false),
      StructField("Volume", IntegerType, false)))
    val df = spark.read.schema(schema).option("header", true).csv("./src/resources/AAPL.csv")
    println("Schema..........")
    df.printSchema()
    df
  }


  def usingAggregateFunctions(spark: SparkSession): Unit = {
    val df = getDataFrame(spark)
    println("Using count.....")
    println(df.count())
    println("Using sum.......")
    df.select(sum((col("age")))).show()
    println("Using groupBy.......")
    df.groupBy(col("name")).count().show(5)
    println("Using agg..........")
    df.groupBy("name").agg(expr("sum(age)"), expr("avg(age)")).show()
    println("Another way of wrtitinng it.......")
    df.groupBy("name").agg("age" -> "sum", "age" -> "avg").show()

    println("Using collect_set..........")
    val nameDf = df.agg(expr("collect_set(name)").alias("nameSet"))
    nameDf.show()
    println("Using explode...........")
    nameDf.select(expr("explode(nameSet)")).show()
  }

  def handlingDateAndTime(spark: SparkSession): Unit = {
    val df = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
    df.createOrReplaceTempView("dateTable")

    println("DataFrame Schema........")
    df.printSchema()

    println("date_sub and date_add.............")
    df.select(expr("today"), date_sub(col("today"), 7).alias("7 DAYS BEFORE"), date_add(col("today"), 7).alias("7 DAYS AFTER")).show(1)

    println("datediff..............")
    df.withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today"))).show(1)

    println("to_date..............")
    df.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show(1)

    println("to_timestamp.........")
    val dateFormat = "yyyy-dd-MM"
    val dateDF = spark.range(1).select(to_date(lit("2017-12-11"), dateFormat).alias("DATE_1"))
    dateDF.select(to_timestamp(col("DATE_1"), dateFormat)).show()

  }

  def stringManipulations(spark: SparkSession): Unit = {
    println("********************USAGE OF STRING-MANIPULATION FUNCTIONS****************************")
    val df = getDataFrame(spark)

    println("Initcap Function............")
    df.select(initcap(col("name"))).show(5)

    println("Use of lower and upper.......................")
    df.select(expr("name").alias("ORIGNAL"), lower(col("name")).alias("LOWER"), upper(col("name")).alias("UPPER")).show(5)

    println("Use of ltrim,rtrim,trim,lpad,rpad ......................")
    df.select(
      ltrim(lit("    HELLO    ")).as("ltrim"),
      rtrim(lit("    HELLO    ")).as("rtrim"),
      trim(lit("    HELLO    ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lpad"),
      rpad(lit("HELLO"), 10, " ").as("rpad")).show(2)

    println("Use of regexp_replace.................")
    df.select(regexp_replace(col("name"), "Will|Hugh|Quark", "Human")).show(10)

    //println("Use of regexp_extract.................")
    //df.select(regexp_extract(col("name"), "(Will|Hugh|Quark)", 1)).show(5)

    println("Use of translate.......................")
    df.select(translate(col("name"), "iuean", "86419")).show(5)

    println("Use of contains........................")
    val containsWill = col("name").contains("Will")
    df.withColumn("hasWill", containsWill).where(containsWill).select("*").show()
  }

  def statFunctions(spark: SparkSession): Unit = {
    println("************************************USAGE OF STAT FUNCTIONS***************************")
    val df = getDataFrame(spark)

    println("Use of pow and sqrt................")
    val fabricatedValue = pow(col("age"), 2) + sqrt(col("totalNumOfFriends"))
    df.select(expr("*"), fabricatedValue.alias("COMPUTED_VALUE")).show(5)

    println("Use of round and bround................")
    df.select(lit("2.5").alias("ORIGNAL"), round(lit("2.5")).alias("ROUND_UP"), bround(lit("2.5")).alias("ROUND_DOWN")).show(1)

    println("Correlation between age and total num of friends................")
    println(df.stat.corr("age", "totalNumOfFriends"))

    println("Describe the DataFrame, i.e; (count, mean, stddev_pop, min, max).....................")
    df.describe().show()

    println("Monotonically Increasing ID..........................")
    df.select(monotonically_increasing_id(), expr("*")).show(5)

    println("Use of Rand................................")
    df.select(rand(5)).show()

    println("Use of RandN...............................")
    df.select(randn(5)).show()
  }


  def writeToADBTable(spark: SparkSession): Unit = {
    println("************************************WRITE DATA FROM DF TO DATABASE***************************")
    val df = getDataFrame(spark)
    var connProps = new Properties();
    connProps.put("user", "postgres")
    connProps.put("password", "adept")
    df.write.mode(SaveMode.Overwrite).jdbc(url = "jdbc:postgresql://localhost:5432/spark_db", table = "public.fake_friends", connProps)
    println("Successfully wrote this df to the db")
    df.show(5)
    val count = df.count()
    println(s"This df has $count rows.")
  }

  def readDFFromDatabase(spark: SparkSession): Unit = {
    println("************************************READ A DF FROM DATABASE**********************************")
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/spark_db")
      .option("user", "postgres")
      .option("password", "adept")
      .option("dbtable", "spark_table")
      .load()

    df.show(3);
  }


  val birthYearUDF: Int => Int = (age: Int) => {
    Year.now().getValue - age
  }

  def executeUDF(spark: SparkSession): Unit = {
    println("************************************EXECUTE A UDF*******************************************")
    spark.udf.register("getBirthYear", birthYearUDF)
    val df = getDataFrame(spark)
    df.createOrReplaceTempView("dfTable")
    spark.sql("select id,name,age,totalNumOfFriends,getBirthYear(age) as Year from dfTable").show(5)
    println("************************************EXECUTE A UDF USING DSL*********************************")
    import spark.implicits._
    val func = udf(birthYearUDF)
    df.select("id", "name", "age", "totalNumOfFriends").withColumn("Year", func($"age")).show(5)

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
    if (newSession.catalog.databaseExists("TEST")) {
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

  def createAManagedTable(spark: SparkSession): Unit = {
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
    spark.sql("CREATE DATABASE TEST")
    spark.sql("USE TEST")
    spark.sql("Create table if not exists managed_fake_friends_table_2 (ID INT, NAME STRING, AGE INT, TOTALNUMOFFRIENDS INT) USING PARQUET")
    val df = getDataFrame(spark)
    df.write.mode(SaveMode.Append) saveAsTable ("managed_fake_friends_table_2")
  }

  def managedTable(spark: SparkSession): Unit = {

    println("************************************MANAGED TABLE********************************************")
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
    spark.sql("CREATE DATABASE TEST")
    spark.sql("USE TEST")
    spark.sql("Create table if not exists managed_fake_friends_table (ID INT, NAME STRING, AGE INT, TOTALNUMOFFRIENDS INT) USING PARQUET")
    val df = getDataFrame(spark)
    df.write.mode(SaveMode.Append).saveAsTable("managed_fake_friends_table")
    spark.sql("select * from managed_fake_friends_table").show(5)
    spark.sql("DROP DATABASE IF EXISTS TEST CASCADE")
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
