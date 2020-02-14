import io.delta.tables._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.Encoders


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

// parquet schema.
val parquetSchema = parquetDf.schema

// get distinct rdd.
val rddDistinct = parquetDf.rdd.map(row => (row.getAs("itemId").asInstanceOf[String], row)).groupByKey().map(t => {
  t._2.iterator.next()
})

// create distinct parquet dataframe.
val parquetDfDistinct = newSpark.createDataFrame(rddDistinct, parquetSchema)

// write distinct delta table.
parquetDfDistinct.write
  .format("delta")
  .option("path", "hdfs://mc/test-delta-table-distinct")
  .mode(SaveMode.Overwrite)
  .saveAsTable("test.test_delta_distinct")


val deltaTable = DeltaTable.forPath(newSpark, "/test-delta-table-distinct")

println("before delta table upsert...")
deltaTable.toDF.show(200)
deltaTable.toDF.count

// create row list.
val rowList = List(
  Row("any-item-id4", 200L, 50000L, Row("added-uid-1000", "cart-event", "1.0.0", 1550887585110L)),
  Row("added-item-id-1000", 400L, 50000L, Row("added-uid-1000", "cart-event", "1.0.0", 1550887585110L))
)

// create rdd from row list.
val addedRdd = newSpark.sparkContext.parallelize(rowList)

// create dataframe for adding row.
val addedDf = newSpark.createDataFrame(addedRdd, parquetSchema)

// do upsert with merge.
deltaTable.as("delta")
  .merge(
    addedDf.as("added"),
    "delta.itemId = added.itemId")
  .whenMatched().updateAll()
  .whenNotMatched().insertAll()
  .execute()

println("after delta table upsert...")
deltaTable.toDF.show(300)
deltaTable.toDF.count


// set result data frame to getBack var.
getBack.setResult(deltaTable.toDF)


