import org.apache.spark.sql.SparkSession


// NOTE: variable 'spark' 가 이미 REPL 에 SparkSession instance 로 생성되었기 때문에
//       개발할때만 uncomment 하고 commit 할 경우는 comment 시킴.
//val spark: SparkSession

// TODO: 사용자 Request 별 Session 을 생성해야 하나....
// create new spark session.
val newSpark = spark.newSession

val parquetDf = newSpark.read.format("parquet")
  .load("/test-event-parquet").cache()

parquetDf.show(5)

// print partition count for the dataframe.
println(s"parquetDf partition count: ${parquetDf.rdd.partitions.length}")

import org.apache.spark.sql.functions.{concat, lit}

var newDf = parquetDf
  .withColumn("id", concat(parquetDf.col("itemId"), lit("-"), parquetDf.col("baseProperties.uid")))
  .withColumnRenamed("itemId", "itemIdRenamed")
  .withColumn("uid", parquetDf.col("baseProperties.uid"))
  .withColumn("eventType", parquetDf.col("baseProperties.eventType"))
  .withColumn("version", parquetDf.col("baseProperties.version"))
  .withColumn("ts", parquetDf.col("baseProperties.ts"))
  .drop("baseProperties")

// print schema as pretty json.
println(s"schema as pretty json: ${newDf.schema.prettyJson}")

// reparition dataframe.
newDf = newDf.repartition(4)

// print partition count for the dataframe.
println(s"after repartitioning, newDf partition count: ${newDf.rdd.partitions.length}")

// show catalyst optimization.
newDf.explain(true)

newDf.show(5)


