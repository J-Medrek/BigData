// Databricks notebook source
import BigData._

// COMMAND ----------

import sys.process._
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

def getfile(url: String, outputFile: String):Unit = {
  val in = new URL(url).openStream
  val out = Paths.get("/tmp/" + outputFile)
  Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING)
}


// COMMAND ----------

val filePath = "FileStore/lab11/"
val url="https://raw.githubusercontent.com/J-Medrek/BigData/main/lab11/actors.csv"
val file = "actors.csv"
val dbfsdestination = "dbfs:/FileStore/lab11/"
val tmp = "file:/tmp/"

dbutils.fs.mkdirs(filePath)

getfile(url,file)
dbutils.fs.mv(tmp + file, dbfsdestination + file)

// COMMAND ----------

val filePath = "FileStore/lab11/"
val url="https://raw.githubusercontent.com/J-Medrek/BigData/main/lab11/names.csv"
val file = "names.csv"
val dbfsdestination = "dbfs:/FileStore/lab11/"
val tmp = "file:/tmp/"

dbutils.fs.mkdirs(filePath)

getfile(url,file)
dbutils.fs.mv(tmp + file, dbfsdestination + file)

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/lab11"))

// COMMAND ----------

val a=Array("dbfs:/FileStore/lab11/actors.csv","dbfs:/FileStore/lab11/names.csv","dbfs:/FileStore/lab11/output")
Main.main(a)
