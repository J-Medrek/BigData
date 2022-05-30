package BigData.Datareader

import BigData.SparkSessionProvider
import org.apache.spark.sql.{Dataset, SparkSession}
import BigData.Case.Names
import BigData.Case.Actors

object ReadFile extends SparkSessionProvider {
  def readNames(spark: SparkSession, path: String): Dataset[Names] = {
    import spark.implicits._
    return spark.read.format("csv").option("header",true).option("delimiter", ";").load(path).as[Names]
  }
  def readActors(spark: SparkSession, path: String): Dataset[Actors] = {
    import spark.implicits._
    return spark.read.format("csv").option("header",true).option("delimiter", ";").load(path).as[Actors]
  }
}
