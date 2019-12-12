import org.apache.spark.sql.{Dataset, Row, RowFactory, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

// NOTE: variable 'spark' 가 이미 REPL 에 SparkSession instance 로 생성되었기 때문에
//       개발할때만 uncomment 하고 commit 할 경우는 comment 시킴.
//val spark: SparkSession

// TODO: 사용자 Request 별 Session 을 생성해야 하나....
// create new spark session.
val newSpark = spark.newSession

val parquetDf = newSpark.read.format("parquet")
  .load("/test-event-parquet").cache()

parquetDf.show(5)

val newDf = parquetDf
  .withColumnRenamed("itemId", "itemIdRenamed")
  .withColumn("uid", parquetDf.col("baseProperties.uid"))
  .withColumn("eventType", parquetDf.col("baseProperties.eventType"))
  .withColumn("version", parquetDf.col("baseProperties.version"))
  .withColumn("ts", parquetDf.col("baseProperties.ts"))
  .drop("baseProperties")

newDf.printSchema()

newDf.show(5)


