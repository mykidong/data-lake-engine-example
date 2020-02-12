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

parquetDf.write
  .format("delta")
  .option("path", "hdfs://mc/test-delta-table")
  .mode(SaveMode.Overwrite)
  .saveAsTable("test.parquet_as_delta")


// create delta table with sql.
spark.sql("CREATE TABLE IF NOT EXISTS test.test_delta2 USING DELTA LOCATION '/test-delta-table'")

val testDeltaDf = spark.sql("select * from test.test_delta2")
println("reading from delta table...")
testDeltaDf.show(10);

// update delta.
// NOTE: UPDATE DOES NOT WORK!!!
//spark.sql("UPDATE test_delta SET quantity = 5 WHERE itemId = 'any-item-id0'")
//println("after updating delta table...")
//spark.sql("select * from test_delta").show(10);

val deltaDf = spark.read
  .format("delta")
  .load("/test-delta-table");

println("reading delta table file..")
deltaDf.show(4);


val deltaSqlDf = spark.sql("select * from test.parquet_as_delta");

println("reading hive delta table..")
deltaSqlDf.show(5)

// set result data frame to getBack var.
getBack.setResult(deltaSqlDf)


