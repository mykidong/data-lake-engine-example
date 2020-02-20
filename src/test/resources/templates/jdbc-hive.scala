import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

// NOTE: variable 'spark' 가 이미 REPL 에 SparkSession instance 로 생성되었기 때문에
//       개발할때만 uncomment 하고 commit 할 경우는 comment 시킴.
//val spark: SparkSession

// TODO: 사용자 Request 별 Session 을 생성해야 하나....
// create new spark session.
val newSpark = spark.newSession



// NOTE: Spark JDBC for Hive 는 Object, Array Type 을 Support 하지 않음!!!

val jdbcDf = newSpark.read.format("jdbc")
  .option("url", "jdbc:hive2://mc-d03.opasnet.io:10016")
  .option("dbtable", "mc.crawl_youtube")
  .option("user", "")
  .option("password", "")
  .option("fetchsize", "100")
  .load

jdbcDf.show(100)

jdbcDf.printSchema()


jdbcDf.write
  .format("delta")
  .option("path", "/test-delta-from-jdbc")
  .mode(SaveMode.Overwrite)
  .saveAsTable("test.test_delta_from_jdbc")