package laby.lab7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Project {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Moja-applikacja")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val path = args(0)

    var df = spark.read.option("header", "true").csv(path)

    df.select("*").show()

    df=df.withColumn("kol4", col("kol3") * 3)

    df.select("*").show()

    df=df.withColumn("kol2",col("kol2")/2)

    df.select("*").show()
  }

}
