## Настройка проекта

#### Создать project/plugins.sbt:

`addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")`



#### Внутри build.sbt в settings вписать:


```
.settings(
// ...
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-sql" % "3.3.2" % Provided
),
assembly / assemblyJarName := "assembly.jar",
)
```



#### Открыть Run - Edit configurations
1. Создать задачу типа sbt Task

**_Name_**: assembly

**_Tasks_**: assembly

**_Use sbt shell_** - убрать галочку

**_Working directory_**: директория проекта

**_VM parameters_**: пусто


#### Создать задачу типа Spark Submit - Local:

**_Name_**: spark-submit

**_Spark home_**: путь до ранее скачанного дистрибутива Spark

**`Application`**: target/scala-2.13/assembly.jar

В секции **_Shell Options_** - **Working Directory**: директория проекта

**_Before launch_**: добавить через Run another configuration задачу assembly
