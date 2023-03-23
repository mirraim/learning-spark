import structured_api.{Bloggers, FireCalls}


// Запускать через spark-submit
// Для выбора нужной сессии вызвать <объект>.<метод>

object Main {

  def main(args: Array[String]): Unit = {
    FireCalls.calls()
  }

}
