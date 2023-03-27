package dataframe.internal_sources

import org.apache.spark.sql.SparkSession

object SourcesAndFormats {

  val spark: SparkSession = SparkSession
    .builder
    .appName("SourcesAndFormats")
    .getOrCreate()

  def execute(): Unit = {

    parquet()

    json()

    csv()

  }

  def parquet(): Unit = {
    // reading Parquet to DataFrame
    val file = """datasets/dataframe/summary-data/parquet/2010-summary.parquet"""
    val df = spark.read.format("parquet").load(file)
    // Use Parquet; you can omit format("parquet") if you wish as it's the default
    val df2 = spark.read.load(file)

    // Reading Parquet files into a Spark SQL table
    spark.sql(
      """
        CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        USING parquet
        OPTIONS (path "datasets/dataframe/summary-data/parquet/2010-summary.parquet" )
        """)

    // writing Parquet
    df.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("/tmp/data/parquet/df_parquet")

    // writing Spark SQL Table
    df.write
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl_1")

  }

  def json(): Unit = {
    // reading JSON to DataFrame
    val df = spark.read.format("json")
      .load("datasets/dataframe/summary-data/json/*")

    // Reading a JSON file into a Spark SQL table
    spark.sql(
      """
        CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        USING json
        OPTIONS (path "datasets/dataframe/summary-data/json/*" )
        """)

    // writing JSON
    df.write.format("json")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("/tmp/data/json/df_json")
  }

  def csv(): Unit = {

    val file = "datasets/dataframe/summary-data/csv/*"
    // reading CSV to DataFrame
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load(file)


    val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
    val df1 = spark.read.format("csv")
      .schema(schema)
      .option("header", "true")
      .option("mode", "FAILFAST")   // Exit if any errors
      .option("nullValue", "")      // Replace any null data with quotes
      .load(file)

    // Reading a CSV file into a Spark SQL table
    val readQuery = s"""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
                USING csv
                OPTIONS (path "$file", header "true", inferSchema "true", mode "FAILFAST"
                )"""

    spark.sql(readQuery)

    // Writing DataFrames to CSV files
    df.write.format("csv").mode("overwrite").save("/tmp/data/csv/df_csv")

  }

  /**
   * В отличие от многих других форматов Big Data, столбцовых (RCFile, Apache ORC и Parquet) и
   * линейно-ориентированных (Sequence, Map-File), Avro поддерживает эволюцию схем данных,
   * обрабатывая изменения схемы путем пропуска, добавления или модификации отдельных полей.
   * Авро не является строго типизированным форматом: информация о типе каждого поля хранится в разделе метаданных вместе со схемой.
   */
  def avro(): Unit = {

    // reading avro to DataFrame
    val df = spark.read.format("avro")
      .load("")
    df.show(false)

    // Reading an Avro file into a Spark SQL table
    spark.sql(
      """
         CREATE OR REPLACE TEMPORARY VIEW episode_tbl
         USING avro
         OPTIONS (path "datasets/dataframe/summary-data/avro/*")
         """)
    // Writing DataFrames to avro files
    df.write
      .format("avro")
      .mode("overwrite")
      .save("/tmp/data/avro/df_avro")
  }

  def image(): Unit = {
    val imageDir = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/" // изображния не сохранены в проект, так что файлы спарк не найдет
    val imagesDF = spark.read.format("image").load(imageDir)
    imagesDF.printSchema
    imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode",
      "label").show(5, false)
  }


  /**
   * binary file data source does not support writing a DataFrame back to the original file format
   */
  def binaryFiles(): Unit = {
    val path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"  // изображния не сохранены в проект, так что файлы спарк не найдет
    val binaryFilesDF = spark.read.format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .load(path)
    binaryFilesDF.show(5)

    // ignore partitioning data discovery
    val binaryDF = spark.read.format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .option("recursiveFileLookup", "true")
      .load(path)
    binaryFilesDF.show(5)
  }

}
