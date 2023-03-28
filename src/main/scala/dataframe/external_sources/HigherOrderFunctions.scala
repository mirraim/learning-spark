package dataframe.external_sources

import org.apache.spark.sql.SparkSession

object HigherOrderFunctions {

  val spark: SparkSession = SparkSession
    .builder
    .appName("Functions")
    .getOrCreate()

  def execute(): Unit = {
    // Create DataFrame with two rows of two arrays (tempc1, tempc2)
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)

    import spark.implicits._

    val tC = Seq(t1, t2).toDF("celsius")
    tC.createOrReplaceTempView("tC")
    // Show the DataFrame
    tC.show()

    // Calculate Fahrenheit from Celsius for an array of temperatures
    // transform(array<T>, function<T, U>): array<U>
    spark.sql("SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC").show()

    // Filter temperatures > 38C for array of temperatures
    // filter(array<T>, function<T, Boolean>): array<T>
    spark.sql("SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC").show()

    // Is there a temperature of 38C in the array of temperatures
    // exists(array<T>, function<T, V, Boolean>): Boolean
    spark.sql("SELECT celsius, exists(celsius, t -> t = 38) as threshold FROM tC ").show()

    // Calculate average temperature and convert to F
    // reduce(array<T>, B, function<B, T, B>, function<B, R>)
    // Это не работает, хз почему
    spark.sql("""SELECT celsius,
              reduce(
              celsius,
              0,
              (t, acc) -> t + acc,
              acc -> (acc div size(celsius) * 9 div 5) + 32 )
              as avgFahrenheit
              FROM tC""").show()
  }
}
