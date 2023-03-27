package dataframe.external_sources

import org.apache.spark.sql.SparkSession

import java.util.Properties

object ExternalConnections {
  val spark: SparkSession = SparkSession
    .builder
    .appName("Connections")
    .getOrCreate()

  def postgres():Unit = {
    // Read Option 1: Loading data from a JDBC source using load method
    val jdbcDF1 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:[DBSERVER]")
      .option("dbtable", "[SCHEMA].[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .load()

    // Read Option 2: Loading data from a JDBC source using jdbc method
    // Create connection properties
    val cxnProp = new Properties()
    cxnProp.put("user", "[USERNAME]")
    cxnProp.put("password", "[PASSWORD]")

    // Load data using the connection properties
    val jdbcDF2 = spark
      .read
      .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)

    // Write Option 1: Saving data to a JDBC source using save method
    jdbcDF1
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql:[DBSERVER]")
      .option("dbtable", "[SCHEMA].[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .save()

    // Write Option 2: Saving data to a JDBC source using jdbc method
    jdbcDF2.write
      .jdbc(s"jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)
  }

  def mySql():Unit = {
    // Loading data from a JDBC source using load
    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .load()

    // Saving data to a JDBC source using save
    jdbcDF
      .write
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .save()
  }

  def msSqlServer(): Unit = {
    // Loading data from a JDBC source
    // Configure jdbcUrl
    val jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"
    // Create a Properties() object to hold the parameters.
    // Note, you can create the JDBC URL without passing in the
    // user/password parameters directly.
    val cxnProp = new Properties()
    cxnProp.put("user", "[USERNAME]")
    cxnProp.put("password", "[PASSWORD]")
    cxnProp.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    // Load data using the connection properties
    val jdbcDF = spark.read.jdbc(jdbcUrl, "[TABLENAME]", cxnProp)
    // Saving data to a JDBC source
    jdbcDF.write.jdbc(jdbcUrl, "[TABLENAME]", cxnProp)
  }

}
