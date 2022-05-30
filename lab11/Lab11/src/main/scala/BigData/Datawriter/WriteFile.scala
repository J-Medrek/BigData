package BigData.Datawriter

import org.apache.spark.sql.DataFrame

object WriteFile {

  def writedf(dt:DataFrame,path: String)= {
    dt.write.format("csv").save(path);
  }

}
