package mykidong.utils

import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class StringUtilsSpec extends FunSuite{

  test("print spark configuration") {

    val sparkConf = new SparkConf().setAppName(getClass.getName)
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse")
    sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "3")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "6")
    sparkConf.set("spark.shuffle.service.enabled", "true")

    println("sparkConf: " + sparkConf.getAll.toList.toString())
  }
}
