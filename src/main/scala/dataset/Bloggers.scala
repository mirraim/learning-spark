package dataset

import org.apache.spark.sql.{SparkSession}

object Bloggers {
  def execute(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Bloggers")
      .getOrCreate()
    import spark.implicits._

    val bloggers = "datasets/blogs.json"
    val bloggersDS = spark
      .read
      .format("json")
      .option("path", bloggers)
      .load()
      .as[Bloggers]
  }


  case class Bloggers(id: Int,
                      first: String,
                      last: String,
                      url: String,
                      date: String,
                      hits: Int,
                      campaigns: Array[String])

}
