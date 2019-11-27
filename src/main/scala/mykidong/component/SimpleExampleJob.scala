package mykidong.component

import mykidong.util.Log4jConfigurer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory

object SimpleExampleJob {

  private val log = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // init. log4j.
    Log4jConfigurer.loadLog4j(null)

    // spark configuration.
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse")
    sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar")

    // set fair scheduler mode.
    sparkConf.set("spark.scheduler.mode", "FAIR")

    // spark session.
    val spark = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate

    val parquetDs = spark.read.format("parquet")
      .load("/test-event-parquet")

    parquetDs.show(3)

    implicit val intEncoder = Encoders.scalaInt
    val sum = parquetDs.map(row => {
      println("row: " + row.toString)

      1
    }).count()

    log.info("sum: " + sum)

    spark.stop()
  }

}
