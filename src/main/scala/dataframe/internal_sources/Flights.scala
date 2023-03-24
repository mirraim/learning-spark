package dataframe.internal_sources

import org.apache.spark.sql.functions.{col, desc, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Flights {
  def fly(): Unit = {

    val spark = SparkSession
      .builder
      .appName("SparkSQLExampleApp")
      .getOrCreate()

    val csvFile="datasets/dataframe/departuredelays.csv"

    // Read and create a temporary view
    // Infer schema (note that for larger files you may want to specify the schema)
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)
    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    bigDistanceSql(spark)
    bigDistanceDF(df)

    bigDelaySql(spark)
    bigDelayDF(df)

    caseSql(spark)
    caseDF(df)


    spark.stop()
  }

  def bigDistanceSql(spark: SparkSession): Unit = {
    spark.sql(
      """SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""")
      .show(10)
  }

  def bigDistanceDF(df: DataFrame): Unit = {
    df
      .select("distance", "origin", "destination")
      .where(col("distance") > 1000)
      .orderBy(desc("distance"))
      .show(10)
  }

  def bigDelaySql(spark: SparkSession): Unit = {
    spark.sql(
      """SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""")
      .show(10)
  }

  def bigDelayDF(df: DataFrame): Unit = {
    df
      .select("date", "delay", "origin", "destination")
      .filter(df("delay") > 120
        && df("origin") === "SFO"
        && df("destination") === "ORD")
      .orderBy(desc("delay"))
      .show(10)
  }

  def caseSql(spark: SparkSession): Unit = {
    spark.sql(
      """SELECT delay, origin, destination,
    CASE
    WHEN delay > 360 THEN 'Very Long Delays'
    WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
    WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
    WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
    WHEN delay = 0 THEN 'No Delays'
    ELSE 'Early'
    END AS Flight_Delays
    FROM us_delay_flights_tbl
    ORDER BY origin, delay DESC""")
      .show(10)
  }

  def caseDF(df: DataFrame): Unit = {
    val caseDf = df
      .select("delay", "origin", "destination")
      .orderBy(col("origin"), desc(("delay")))

    caseDf.withColumn("Flight_Delays",
      when(col("delay") > 360, "Very Long Delays")
        .when( duration(120, 360), "Long Delays")
        .when( duration(60, 120), "Short Delays")
        .when( duration(0, 60), "Tolerable Delays")
        .when(col("delay") === 0, "No Delays")
        .otherwise("Early"))
      .show(10)

  }

  def duration(min: Int, max: Int): Column = {
    col("delay") > min && col("delay") < max
  }

}
