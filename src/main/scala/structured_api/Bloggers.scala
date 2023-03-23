package structured_api

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
object Bloggers {

  def schema(): Unit = {
    val spark = SparkSession
      .builder
      .appName("CreateSchema")
      .getOrCreate()

    val jsonFile = "datasets/blogs.json"

    val schema = rddSchema()
    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read
      .schema(schema)
      .json(jsonFile)
    // Show the DataFrame schema as output
    blogsDF.show(false)

    // Print the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)

    columns(blogsDF)

    // Row to DataFrame
    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))

    import spark.implicits._
    val authorsDF = rows.toDF("Author", "State")
    authorsDF.show()

    spark.close()
  }

  /**
   * Schema RDD (Resilient Distributed Dataset) - low-level
   */
  private def rddSchema(): StructType = {
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))
    schema
  }

  /**
   * Schema DDL (Data Definition Language) - high-level
   */
  private def ddlSchema(): String = {
    val string = "STRING"
    val int = "INT"
    s"Id $int, First $string, Last $string, Url $string, Published $string, Hits $int, Campaigns ARRAY<$string>"
  }

  /**
   * Работа с колонками
   */
  private def columns(blogsDF: DataFrame): Unit = {
    blogsDF.columns
    blogsDF.col("Id")
    // Use an expression to compute a value
    blogsDF.select(expr("Hits * 2")).show(2)
    // or use col to compute value
    blogsDF.select(col("Hits") * 2).show(2)

    // Use an expression to compute big hitters for blogs
    // This adds a new column, Big Hitters, based on the conditional expression
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    // Concatenate three columns, create a new column, and show the
    // newly created concatenated column
    blogsDF
      .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
      .select(col("AuthorsId"))
      .show(4)

    // These statements return the same value, showing that
    // expr is the same as a col method call
    blogsDF.select(expr("Hits")).show(2)
    blogsDF.select(col("Hits")).show(2)
    blogsDF.select("Hits").show(2)

    // Sort by column "Id" in descending order
    blogsDF.sort(col("Id").desc).show()
  }

  private def rows(): Unit = {
    // Create a Row
    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
      Array("twitter", "LinkedIn"))
    // Access using index for individual items
    blogRow(1)
  }
}
