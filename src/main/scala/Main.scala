import dataframe.external_sources.Udf


// Запускать через spark-submit
// Для выбора нужной сессии вызвать <объект>.<метод>

object Main {

  def main(args: Array[String]): Unit = {
    Udf.execute()
  }

}
