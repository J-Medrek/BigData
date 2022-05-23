// Databricks notebook source
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

val filePath = "FileStore/lab9/"
val url="https://raw.githubusercontent.com/J-Medrek/BigData/9c5fe749f5d86120eaf88647e4e5ed5b379fd6a5/lab9/Nested.json"
val file = "Nested.json"
val dbfsdestination = "dbfs:/FileStore/lab9/"
val tmp = "file:/tmp/"

dbutils.fs.mkdirs(filePath)

getfile(url,file)
dbutils.fs.mv(tmp + file, dbfsdestination + file)


// COMMAND ----------

val js=spark.read.format("json").option("multiline", "true").load("dbfs:/FileStore/lab9/Nested.json")

// COMMAND ----------

display(js)

// COMMAND ----------

display(js.withColumn("nowa", $"pathLinkInfo".dropFields("alternateName")))

// COMMAND ----------

// MAGIC %md
// MAGIC foldLeft iteruje po strukturze danych i wykonuje jakieś operacje na jej elementach od lewej do prawej

// COMMAND ----------

//poniżej przykład sumowania elementów listy
//0 początkowa wartość, i to pierwszy element struktury, w każdej iteracji wynik zostaje zapisany do z a i to następne elementy
val numbers = List(5, 4, 8, 6, 2)
numbers.fold(0) { (z, i) =>
  z + i
}



// COMMAND ----------

//Przykład 2

//obiekt Foo mający nazwę, wiek i płeć
//za pomocą foldLeft tworzymy listę napisów "[Mrs.|Mr.][name],[age]"
//początkową wartością jest pusta lista
//z każdą iteracją dodajemy do niej nowy napis

class Foo(val name: String, val age: Int, val sex: Symbol)

object Foo {
  def apply(name: String, age: Int, sex: Symbol) = new Foo(name, age, sex)
}
val fooList = Foo("Hugh Jass", 25, 'male) ::
              Foo("Biggus Dickus", 43, 'male) ::
              Foo("Incontinentia Buttocks", 37, 'female) ::
              Nil
val stringList = fooList.foldLeft(List[String]()) { (z, f) =>
  val title = f.sex match {
    case 'male => "Mr."
    case 'female => "Ms."
  }
  z :+ s"$title ${f.name}, ${f.age}"
}


// COMMAND ----------

//Przykład 3
//Czyscimy napisy ze znaków specjalnych w kolumnach tabeli
//Początkowa wartość to tabela i dla każdej iteracji(kolumny) usuwamy znaki specjalne dla każdej wartości, w każdej iteracji tabela wynikowa zostaje następną wartością

val sourceDF = Seq(
  ("  p a   b l o", "Paraguay"),
  ("Neymar", "B r    asil")
).toDF("name", "country")

val actualDF = Seq(
  "name",
  "country"
).foldLeft(sourceDF) { (memoDF, colName) =>
  memoDF.withColumn(
    colName,
    regexp_replace(col(colName), "\\s+", "")
  )
}

actualDF.show()

// COMMAND ----------

val fields = Array("alternateName","captureSpecification")

val z=fields.foldLeft(js) { (df, col) =>
  df.withColumn(
    "new",
    $"pathLinkInfo".dropFields(col))
}


// COMMAND ----------

display(z)
