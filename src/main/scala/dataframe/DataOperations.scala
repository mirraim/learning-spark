package dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object DataOperations {

  val spark: SparkSession = SparkSession
    .builder
    .appName("Functions")
    .getOrCreate()

  def execute(): Unit = {
    // Set file paths
    val delaysPath =
      "datasets/dataframe/departuredelays.csv"
    val airportsPath =
      "datasets/dataframe/airport-codes-na.txt"

    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")

    // Obtain departure Delays data set
    val delays = spark.read
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    // Create temporary small table
    val foo = delays.filter(expr("""
                                origin == 'SEA'
                                AND destination == 'SFO'
                                AND date like '01010%'
                                AND delay > 0
                                """))
    foo.createOrReplaceTempView("foo")

    airports.show(10)
    delays.show(10)
    foo.show(10) // В выводе будет всего 3 строки

    union(delays, foo)

    join(airports, foo)

    modificate(foo)

  }

  def union(delays: DataFrame, foo: DataFrame): Unit = {
    // Union two tables
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr(
      """origin == 'SEA' AND destination == 'SFO'
    AND date LIKE '01010%' AND delay > 0""")).show()
  }

  def join(airports: DataFrame, foo: DataFrame): Unit = {
    foo.join(
      airports.as("air"),
      col("air.IATA") === col("origin"))
      .select("City", "State", "date", "delay", "distance", "destination")
      .show()
  }

  def modificate(foo: DataFrame): Unit = {

    // Adding new columns
    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )

    // Dropping columns
    val foo3 = foo2.drop("delay")
    foo3.show()

    // Renaming columns
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

    // Pivoting
    spark.sql(
      """
        SELECT * FROM (
          SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
          FROM departureDelays WHERE origin = 'SEA')
        PIVOT (
          CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
          FOR month IN (1 JAN, 2 FEB))
        ORDER BY destination
        """).show(20)
  }

}
