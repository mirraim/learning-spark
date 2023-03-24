package structured_api

import org.apache.spark.sql.SparkSession

object InternetOfThings {

  def things(): Unit =  {
    val spark = SparkSession
      .builder
      .appName("IoT")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read
      .json("datasets/iot_devices.json")
      .as[DeviceIoTData]

    ds.show(5, false)

    val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70}})
    filterTempDS.show(5, false)

    // Вместо SQL-подобного запроса Dataset можно преобразовывать выражениями Scala или Java
    val dsTemp = ds
      .filter(d => {
        d.temp > 25
      })
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]
    dsTemp.show(5, false)

// Но можно и спользовать тот же стиль, что и DataFrame
    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .where("temp > 25")
      .as[DeviceTempByCountry]
    dsTemp2.show(5, false)

    val device = dsTemp.first()
    println(device)

    spark.stop()
  }

  /**
   * schema for Dataset[DeviceToDate]
   */
  case class DeviceIoTData(battery_level: Long,
                                   c02_level: Long,
                                   cca2: String,
                                   cca3: String,
                                   cn: String,
                                   device_id: Long,
                                   device_name: String,
                                   humidity: Long,
                                   ip: String,
                                   latitude: Double,
                                   lcd: String,
                                   longitude: Double,
                                   scale: String,
                                   temp: Long,
                                   timestamp: Long)

  /**
   * case-класс для language-native преобразований
   */
  case class DeviceTempByCountry(temp: Long,
                                           device_name: String,
                                           device_id: Long,
                                           cca3: String)

}
