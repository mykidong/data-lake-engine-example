package org.apache.spark.specs

import java.util.Properties

import mykidong.http.SimpleHTTPServer
import mykidong.util.Log4jConfigurer
import org.apache.spark.SparkConf
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.support.PropertiesLoaderUtils

class InteractiveHandlerApplicationSpec extends FunSuite{

  private val log = LoggerFactory.getLogger(classOf[InteractiveHandlerApplicationSpec])

  test("runSparkOnLocal") {

    // init. log4j.
    Log4jConfigurer.loadLog4j(null)

    // spark configuration for local mode.

    val sparkConf = new SparkConf().setAppName(getClass.getName)
    sparkConf.setMaster("local[2]")

    sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse")

    // IMPORTANT FOR HIVE SUPPORT!!!
    sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar")

    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "3")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "6")
    sparkConf.set("spark.shuffle.service.enabled", "true")


    // set fair scheduler mode.
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.scheduler.allocation.file", "/usr/lib/mc/conf/fairscheduler.xml")

    // hadoop configuration.
    val resource = new ClassPathResource("hadoop-conf.properties")
    val hadoopProps = PropertiesLoaderUtils.loadProperties(resource)

    // hive configuration.
    val hiveProps = PropertiesLoaderUtils.loadProperties(new ClassPathResource("hive-conf.properties"))

    val propsArray = new Array[Properties](2)
    propsArray(0) = hadoopProps
    propsArray(1) = hiveProps


    // run embeded http server.
    val port = 8125
    SimpleHTTPServer.run(sparkConf, propsArray, port)
    log.info("embedded http server is running now ...")

    Thread.sleep(Long.MaxValue)
  }

}
