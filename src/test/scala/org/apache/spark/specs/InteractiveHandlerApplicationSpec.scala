package org.apache.spark.specs

import java.util.UUID

import mykidong.http.SimpleHTTPServer
import mykidong.util.Log4jConfigurer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils
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
    /**
     * ReplMain 과 똑같이 마춤.
     *
     * rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
     * outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")
     */
    val rootDir = "/tmp/spark-" + UUID.randomUUID().toString
    val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")

    val sparkConf = new SparkConf().setAppName(getClass.getName)
    sparkConf.setMaster("local[2]")

    // spark repl class uri 을 생성하기 위함.
    sparkConf.set("spark.repl.classdir", rootDir)
    sparkConf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse")
    sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "3")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "6")
    sparkConf.set("spark.shuffle.service.enabled", "true")


    // set fair scheduler mode.
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.scheduler.allocation.file", "/usr/lib/mc/conf/fairscheduler.xml")

    val spark = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate

    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration

    // set hadoop configuration.

    // hadoop configuration.
    val resource = new ClassPathResource("hadoop-conf.properties")
    val hadoopProps = PropertiesLoaderUtils.loadProperties(resource)

    import scala.collection.JavaConversions._
    for (key <- hadoopProps.stringPropertyNames) {
      val value = hadoopProps.getProperty(key)
      hadoopConfiguration.set(key, value)
    }

    // hive configuration.
    val hiveProps = PropertiesLoaderUtils.loadProperties(new ClassPathResource("hive-conf.properties"))
    import scala.collection.JavaConversions._
    for (key <- hiveProps.stringPropertyNames) {
      val value = hiveProps.getProperty(key)
      hadoopConfiguration.set(key, value)
    }


    // run embeded http server.
    val port = 8125
    SimpleHTTPServer.run(sparkConf, port)
    log.info("embedded http server is running now ...")

    Thread.sleep(Long.MaxValue)
  }

}
