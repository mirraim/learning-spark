package dataframe.external_sources

import org.apache.spark.sql.SparkSession

object Udf {

  val spark: SparkSession = SparkSession
    .builder
    .appName("UDF")
    .getOrCreate()

  def execute(): Unit = {
    // Create cubed function
    val cubed = (s: Long) => {
      s * s * s
    }
    // Register UDF
    spark.udf.register("cubed", cubed)
    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
  }

}
