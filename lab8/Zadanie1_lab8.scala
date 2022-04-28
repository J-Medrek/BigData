// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC   

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val fileName = "dbfs:/databricks-datasets/wikipedia-datasets/data-001/pageviews/raw"

val data = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

data.write.mode("overwrite").parquet("dbfs:/wikipedia.parquet")

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

display(data)

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "dbfs:/wikipedia.parquet"

val df = spark.read
  .parquet(parquetDir)
.repartition(2000)
    //.coalesce(2)
.groupBy("site").sum()


df.explain
val p=df.count()
//printRecordsPerPartition(df)

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC * 8 partycji 8,59s
// MAGIC * 1 partycja 6,34s
// MAGIC * 7 partycja 8,17s
// MAGIC * 9 partycja 9,97s
// MAGIC * 16 partycja 9,93s
// MAGIC * 24 partycja 9,14s
// MAGIC * 96 partycja 10,63s
// MAGIC * 200 partycja 12,96s
// MAGIC * 4000 partycja 3,55 min
// MAGIC 
// MAGIC * 6 partycji 4,17s
// MAGIC * 5 partycji 4,03s
// MAGIC * 4 partycji 3,83s 
// MAGIC * 3 partycji 4,38s
// MAGIC * 2 partycji 5,14s
// MAGIC * 1 partycji 5,42s
