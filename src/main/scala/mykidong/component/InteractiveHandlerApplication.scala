package mykidong.component

import mykidong.http.SimpleHTTPServer
import mykidong.util.Log4jConfigurer
import org.apache.spark.SparkConf
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

    // NOTE: Yarn 에서는 잘 돌지만 개발 현재 Spark Cluster 에서는 동작을 안하고 Error 발생함!!!!
//    sparkConf.set("spark.dynamicAllocation.enabled", "true")
//    sparkConf.set("spark.dynamicAllocation.minExecutors", "0")
//    sparkConf.set("spark.dynamicAllocation.maxExecutors", "5")
//    sparkConf.set("spark.shuffle.service.enabled", "true")


    // set fair scheduler mode.
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.scheduler.allocation.file", "/usr/lib/mc/conf/fairscheduler.xml")

    // run embeded http server.
    val port = 8125
    SimpleHTTPServer.run(sparkConf, port)
    log.info("embedded http server is running now ...")
  }
}