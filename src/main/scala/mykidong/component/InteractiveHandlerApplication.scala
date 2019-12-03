package mykidong.component

import mykidong.http.SimpleHTTPServer
import mykidong.util.Log4jConfigurer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object InteractiveHandlerApplication {

  private val log = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // init. log4j.
    Log4jConfigurer.loadLog4j(null)

    // spark configuration.
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse")
    sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "3")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "6")

    // set fair scheduler mode.
    sparkConf.set("spark.scheduler.mode", "FAIR")

    // spark session.
    val spark = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate

    // run embeded http server.
    val port = 8125
    SimpleHTTPServer.run(spark, spark.sparkContext, port)
    log.info("embedded http server is running now ...")

    Thread.sleep(Long.MaxValue)

    spark.stop()
  }
}