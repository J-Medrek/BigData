// Databricks notebook source
// MAGIC %md
// MAGIC 1. Pobierz dane Spark-The-Definitive_Guide dostępne na github
// MAGIC 2. Użyj danych do zadania '../retail-data/all/online-retail-dataset.csv'

// COMMAND ----------

// Pobranie pliku

import sys.process._
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.spark.sql.types.{
    StructType, StructField, DateType,StringType,IntegerType,DecimalType,DataTypes,ShortType,VarcharType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

def getfile(url: String, outputFile: String):Unit = {
  val in = new URL(url).openStream
  val out = Paths.get("/tmp/" + outputFile)
  Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING)
}

val filePath = "FileStore/lab10/"
val url="https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv"
val file = "dane.csv"
val dbfsdestination = "dbfs:/FileStore/lab10/"
val tmp = "file:/tmp/"

dbutils.fs.mkdirs(filePath)

getfile(url,file)
dbutils.fs.mv(tmp + file, dbfsdestination + file)

// COMMAND ----------

val df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/lab10/dane.csv")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Zapisz DataFrame do formatu delta i stwórz dużą ilość parycji (kilkaset)
// MAGIC * Partycjonuj po Country

// COMMAND ----------

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("Country").save("/FileStore/lab10/delta/dane")

// COMMAND ----------

val df=spark.read.format("delta").option("header","true").load("/FileStore/lab10/delta/dane")
display(df)

// COMMAND ----------

spark.catalog.clearCache()

val part=df
.repartition(600)
.groupBy("country")

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS TabelaRaw")

spark.sql(s"""
  CREATE TABLE TabelaRaw
  USING Delta
  LOCATION '/FileStore/lab10/delta/dane/'
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1: OPTIMIZE and ZORDER
// MAGIC 
// MAGIC Wykonaj optymalizację do danych stworzonych w części I `../delta/retail-data/`.
// MAGIC 
// MAGIC Dane są partycjonowane po kolumnie `Country`.
// MAGIC 
// MAGIC Przykładowe zapytanie dotyczy `StockCode`  = `22301`. 
// MAGIC 
// MAGIC Wykonaj zapytanie i sprawdź czas wykonania. Działa szybko czy wolno 
// MAGIC 
// MAGIC Zmierz czas zapytania kod poniżej - przekaż df do `sqlZorderQuery`.

// COMMAND ----------

// TODO
def timeIt[T](op: => T): Float = {
 val start = System.currentTimeMillis
 val res = op
 val end = System.currentTimeMillis
 (end - start) / 1000.toFloat
}

val sqlZorderQuery = timeIt(spark.sql("select * from TabelaRaw where StockCode = '22301'").collect())

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from TabelaRaw where StockCode = '22301'

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Skompaktuj pliki i przesortuj po `StockCode`.

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE tabelaRaw
// MAGIC ZORDER by (StockCode)

// COMMAND ----------

// MAGIC %md
// MAGIC Uruchom zapytanie ponownie tym razem użyj `postZorderQuery`.

// COMMAND ----------

// TODO
val postZorderQuery = timeIt(spark.sql("select * from TabelaRaw where StockCode = '22301'").collect())

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2: VACUUM
// MAGIC 
// MAGIC Policz liczbę plików przed wykonaniem `VACUUM` for `Country=Sweden` lub innego kraju

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/lab10/delta/dane/Country=Sweden"))

// COMMAND ----------

// TODO
val plikiPrzed = dbutils.fs.ls("dbfs:/FileStore/lab10/delta/dane/Country=Sweden").length

// COMMAND ----------

// MAGIC %md
// MAGIC Teraz wykonaj `VACUUM` i sprawdź ile było plików przed i po.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC VACUUM tabelaRaw RETAIN 192 HOURS

// COMMAND ----------

// MAGIC %md
// MAGIC Policz pliki dla wybranego kraju `Country=Sweden`.

// COMMAND ----------

// TODO
val plikiPo = dbutils.fs.ls("dbfs:/FileStore/lab10/delta/dane/Country=Sweden").length

// COMMAND ----------

// MAGIC %md
// MAGIC ## Przeglądanie histrycznych wartośći
// MAGIC 
// MAGIC możesz użyć funkcji `describe history` żeby zobaczyć jak wyglądały zmiany w tabeli. Jeśli masz nową tabelę to nie będzie w niej history, dodaj więc trochę danych żeby zoaczyć czy rzeczywiście się zmieniają. 

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history tabelaRaw
