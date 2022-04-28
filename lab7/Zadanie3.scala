import org.apache.spark.sql.SparkSession


object Zadanie3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Hive Example")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val path = "plik.txt"

    val df = spark.read.option("header", "true").csv(path)

   df.write
     .format("hive")
     .mode("overwrite")
     .saveAsTable("plik")
    spark.sql("SELECT * FROM plik").show()

    val x=spark.catalog.listTables()
    x.show()

   val truncate_all_tables = (db: String) => {
    val a=spark.catalog.listTables()
    val b=a.select("name").collect()
    spark.sql(f"USE $db")
    for (e <- b)
    {
     val name=e.getString(0)
     val z=spark.catalog.getTable(name)
     val query=f"TRUNCATE TABLE $name"
     spark.sql(query)
    }
   }

truncate_all_tables("default")
   spark.sql("SELECT * FROM plik").show()

  }
}
