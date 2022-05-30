package BigData

import BigData.Datareader.ReadFile
import BigData.Datawriter.WriteFile
import BigData.Transformations.Transformations

object Main extends SparkSessionProvider {

  def main(args: Array[String]): Unit = {
    val actorsDf=ReadFile.readActors(spark,args(0));
    val namesDf=ReadFile.readNames(spark,args(1));
    val filt_actors=actorsDf.filter(row=>BigData.Transformations.Transformations.isactress(row))
    val filt_names=namesDf.filter(row=>BigData.Transformations.Transformations.tallerthan(row,165))
    print("All actress: \n")
    filt_actors.show()
    print("Everyone taller than 165 cm\n")
    filt_names.show()
    Datawriter.WriteFile.writedf(filt_actors.toDF(),args(2));
  }

}
