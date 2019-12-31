package mykidong.interpreter

import org.apache.spark.sql.{Dataset, Row}

object GetBack {

  private var getBack: Dataset[Row] = _

  def setResult(result: Dataset[Row]): Unit = {
    this.getBack = result
  }


  def getResult(): Dataset[Row] = {
    this.getBack
  }
}
