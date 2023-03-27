package dataframe.internal_sources

import org.apache.spark.sql.{DataFrame, SparkSession}

object TablesAndViews {
  def execute(): Unit = {
    val spark = SparkSession
      .builder
      .appName("TablesAndViews")
      .getOrCreate()

    val flightDf = init(spark)
    //tables(flightDf)

    views(flightDf)

    dropViews(spark)

    // Можно получить существующую таблицу из Spark
    val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
    val usFlightsDF2 = spark.table("us_delay_flights_tbl")

    spark.stop()
  }

  def init(spark: SparkSession): DataFrame = {

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    val csvFile = "datasets/dataframe/departuredelays.csv"
    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val flightDf = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(csvFile)
    flightDf
  }

  def tables(flightDf: DataFrame): Unit = {
    // Create managed table
    flightDf.write.saveAsTable("managed_us_delay_flights_tbl")

    // Create unmanaged table
    flightDf
      .write
      .option("path", "/tmp/data/us_flights_delay")
      .saveAsTable("us_delay_flights_tbl")
  }

  /**
   * Create a temporary and global temporary view
   */
  def views(flightDf: DataFrame): Unit = {
    //        dfSfo = spark.sql("SELECT date, delay, origin, destination
    //        FROM us_delay_flights_tbl WHERE origin = 'SFO'")
    val dfSfo = flightDf
      .select("date", "delay", "origin", "destination")
      .filter(flightDf("origin") === "SFO")
    dfSfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")


    //        dfJfk = spark.sql("SELECT date, delay, origin, destination
    //        FROM us_delay_flights_tbl WHERE origin = 'JFK'")
    val dfJfk = flightDf
      .select("date", "delay", "origin", "destination")
      .filter(flightDf("origin") === "JFK")

    dfJfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")
  }

  def sqlViews(spark: SparkSession): Unit = {
    spark.sql(
      """
             CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
             SELECT date, delay, origin, destination
             from us_delay_flights_tbl
             WHERE origin = 'SFO';

             CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
             SELECT date, delay, origin, destination
             from us_delay_flights_tbl
             WHERE origin = 'JFK'
             """)
  }

  def dropViews(spark: SparkSession): Unit = {
    spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")
  }
}
