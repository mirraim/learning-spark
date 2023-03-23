package structured_api

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.{functions => F}

object FireCalls {

  def calls(): Unit = {
    val spark = SparkSession
      .builder
      .appName("FireCalls")
      .getOrCreate()

    // Read the file using the CSV DataFrameReader
    val sfFireFile="datasets/sf-fire-calls.csv"
    val fireDF = spark.read
      .schema(fireSchema())
      .option("header", "true")
      .csv(sfFireFile)

    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")
    fewFireDF.show(5, truncate = false)

    selectByCallType(fireDF)

    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where(col("ResponseDelayedinMins") > 5)
      .show(5, false)

   transformDate(newFireDF)
  }

  private def selectByCallType(fireDF: DataFrame): Unit = {
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")
      .show()

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      .show(10, false)
  }

  private def transformDate(newFireDF: DataFrame): Unit = {
    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")
    // Select the converted columns
    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    fireTsDF
      .select(year(col("IncidentDate")))
      .distinct()
      .orderBy(year(col("IncidentDate")))
      .show()

    //aggregations
    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    //descriptive statistical methods
    fireTsDF
      .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
      .show()
  }

  private def fireSchema(): StructType= {
    val fireSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)))
    fireSchema
  }

  def write(fireDF: DataFrame): Unit = {
    // save as a Parquet file
    val parquetPath = "/some_path"
    fireDF.write.format("parquet").save(parquetPath)

    // save as a table
    val parquetTable = "/some_path" // name of the table
    fireDF.write.format("parquet").saveAsTable(parquetTable)
  }

}
