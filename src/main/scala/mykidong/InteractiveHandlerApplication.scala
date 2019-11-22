package mykidong

import mykidong.component.RequestHandlerApplication
import mykidong.http.RequestHandlerHttpServer
import mykidong.util.Log4jConfigurer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object InteractiveHandlerApplication {

  private val log = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // init. log4j.
    val log4j = new Log4jConfigurer
    log4j.setConfPath("/log4j.xml")
    try log4j.afterPropertiesSet()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }

    // spark configuration.
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse")
    sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar")

    // set fair scheduler mode.
    sparkConf.set("spark.scheduler.mode", "FAIR")

    // spark session.
    val spark = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate

    val jsc = new JavaSparkContext(spark.sparkContext)

    // run embeded http server.
    val port = 8125
    val httpServer = new RequestHandlerHttpServer(port, jsc, spark)
    try {
      httpServer.start()
      log.info("embed http server is running now....")
      Thread.sleep(Long.MaxValue)
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        throw new RuntimeException(e)
    }

    spark.stop()
  }
}