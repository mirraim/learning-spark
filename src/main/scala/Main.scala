import dataframe.internal_sources.{Flights, TablesAndViews}


// Запускать через spark-submit
// Для выбора нужной сессии вызвать <объект>.<метод>

object Main {

  def main(args: Array[String]): Unit = {
    TablesAndViews.execute()
  }

}
