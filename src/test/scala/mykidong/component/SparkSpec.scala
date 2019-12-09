package mykidong.component

import mykidong.util.Log4jConfigurer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.support.PropertiesLoaderUtils

class SparkSpec extends FunSuite{

  private val log = LoggerFactory.getLogger(classOf[SparkSpec])

  test("runReduce") {

    // init. log4j.
    Log4jConfigurer.loadLog4j(null)

    // spark configuration for local mode.
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse")
    sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar")

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

    import net.liftweb.json.JObject
    import net.liftweb.json.JsonAST._
    import net.liftweb.json.JsonDSL._

    val json: JObject = "spark confs" -> spark.sparkContext.getConf.getAll.toList

    println("spark configuration: " + prettyRender(json))


    val data = Array(1, 2, 3, 4, 5)
    val distData = spark.sparkContext.parallelize(data)
    val sum = distData.reduce((a, b) => a + b)
    log.info("sum: [" + sum + "]")
  }

}
