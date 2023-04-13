package dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Usage {
    def execute(): Unit = {

      val spark = SparkSession
        .builder
        .appName("Bloggers")
        .getOrCreate()

      import spark.implicits._

      val r = new scala.util.Random(42)

      // Create 1000 instances of scala Usage class
      // This generates data on the fly
      val data = for (i <- 0 to 1000)
        yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
          r.nextInt(1000)))

      // Create a Dataset of Usage typed data
      val dsUsage = spark.createDataset(data)
      dsUsage.show(10)

      dsUsage
        .filter(d => d.usage > 900)
        .orderBy(desc("usage"))
        .show(5, false)

      dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)

      // Use an if-then-else lambda expression and compute a value
      dsUsage.map(u => {
        if (u.usage > 750) u.usage * .15 else u.usage * .50
      })
        .show(5, false)

      // Use the function as an argument to map()
      dsUsage.map(u => {
        computeCostUsage(u.usage)
      }).show(5, false)

      // Use map() on our original Dataset
      dsUsage.map(u => {
        computeUserCostUsage(u)
      }).show(5)
    }

  private def filterWithUsage(u: Usage): Boolean = u.usage > 900

  // Define a function to compute the usage
  private def computeCostUsage(usage: Int): Double = {
    if (usage > 750) usage * 0.15 else usage * 0.50
  }

  // Compute the usage cost with Usage as a parameter
  // Return a new object, UsageCost
  def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
    UsageCost(u.uid, u.uname, u.usage, v)
  }

  case class Usage(uid: Int, uname: String, usage: Int)

  // Create a new case class with an additional field, cost
  case class UsageCost(uid: Int, uname: String, usage: Int, cost: Double)
}
