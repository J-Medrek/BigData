// Databricks notebook source
// MAGIC %md
// MAGIC https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Indexing
// MAGIC 
// MAGIC Hive nie wspiera indeksowania od 3.0
// MAGIC 
// MAGIC I. Tworzenie materialized views
// MAGIC Zapisują na dysku dane nie tylko wyrażenie jak w przypadku normalnych widoków dlatego używać je powinno sie tylko gdy tabela nie jest często edytowana
// MAGIC np.
// MAGIC 
// MAGIC CREATE MATERIALIZED VIEW widok
// MAGIC PARTITIONED ON (kol1)
// MAGIC STORED AS ORC
// MAGIC AS
// MAGIC SELECT * FROM tab_hive;
// MAGIC 
// MAGIC 
// MAGIC II. Używanie formatów bazujących na kolumnach a nie wierszach np. parquet

// COMMAND ----------

//Napisz funkcję, która usunie danych ze wszystkich tabel w konkretniej bazie danych. Informacje pobierz z obiektu Catalog

// COMMAND ----------

import laby.lab7._

// COMMAND ----------

import sys.process._
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

def getfile(url: String, outputFile: String):Unit = {
  val in = new URL(url).openStream
  val out = Paths.get("/tmp/" + outputFile)
  Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING)
}

val filePath = "FileStore/lab7/"
val url="https://raw.githubusercontent.com/J-Medrek/BigData/main/lab7/plik.txt"
val file = "plik.txt"
val dbfsdestination = "dbfs:/FileStore/lab7/"
val tmp = "file:/tmp/"

dbutils.fs.mkdirs(filePath)

getfile(url,file)
dbutils.fs.mv(tmp + file, dbfsdestination + file)


// COMMAND ----------

val a=Array("dbfs:/FileStore/lab7/plik.txt")
Project.main(a)
