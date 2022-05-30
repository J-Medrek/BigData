package BigData.Transformations

import BigData.Case.Names
import BigData.Case.Actors

object Transformations {

  def isactress(actors_row: Actors): Boolean = {
    if(actors_row.category==null) return false;
    return actors_row.category=="actress";
  }

  def tallerthan(names_row: Names,h:Int): Boolean = {
    if(names_row.height==null) return false;
    return Integer.valueOf(names_row.height)>h;
  }

}
