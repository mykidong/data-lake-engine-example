import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.spark.sql.{SaveMode, SparkSession}


// NOTE: variable 'spark' 가 이미 REPL 에 SparkSession instance 로 생성되었기 때문에
//       개발할때만 uncomment 하고 commit 할 경우는 comment 시킴.
//val spark: SparkSession


// TODO: 개발 현재 delta laks 는 Spark 2.4.2 가 필요함으로 지금의 Spark Cluster(2.3.2) 에는 적용을 할수 없음.

// TODO: 사용자 Request 별 Session 을 생성해야 하나....
// create new spark session.
val newSpark = spark.newSession

val parquetDf = newSpark.read.format("parquet")
  .load("/test-event-parquet").cache()

println("reading parquet file..")
parquetDf.show(5)


val igniteConfPath = "/home/mc/ignite/apache-ignite-2.7.6-bin/examples/config/example-ignite.xml"

// write dataframe to ignite.
parquetDf.write
  .format(IgniteDataFrameSettings.FORMAT_IGNITE)
  .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, igniteConfPath)
  .option(IgniteDataFrameSettings.OPTION_TABLE, "test_event_ignite")
  .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "itemId")
  .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
  .mode(SaveMode.Overwrite)
  .save();


// read dataframe from ignite.
val igniteDf = spark.read
  .format(IgniteDataFrameSettings.FORMAT_IGNITE)               // Data source type.
  .option(IgniteDataFrameSettings.OPTION_TABLE, "test_event_ignite")      // Table to read.
  .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, igniteConfPath) // Ignite config.
  .load()


igniteDf.createOrReplaceTempView("test_event_ignite")

val igniteSqlDf = spark.sql("select * from test_event_ignite");

println("reading hive delta table..")
igniteSqlDf.show(100)

// set result data frame to getBack var.
getBack.setResult(igniteSqlDf)


