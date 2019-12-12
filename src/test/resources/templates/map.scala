import org.apache.spark.sql.{Dataset, Row, RowFactory, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

// NOTE: variable 'spark' 가 이미 REPL 에 SparkSession instance 로 생성되었기 때문에
//       개발할때만 uncomment 하고 commit 할 경우는 comment 시킴.
//val spark: SparkSession

// TODO: 사용자 Request 별 Session 을 생성해야 하나....
// create new spark session.
val newSpark = spark.newSession

val parquetDs = newSpark.read.format("parquet")
  .load("/test-event-parquet").cache()

parquetDs.show(5)

import org.apache.spark.sql.Encoders

case class Event(itemId: String,
                 quantity: Long,
                 price: Long,
                 uid: String,
                 eventType: String,
                 version: String,
                 ts:Long)

val newEventDs = parquetDs.map((row: Row) => {
    val itemId = row.getString(0)
    val quantity = row.getLong(1)
    val price = row.getLong(2)
    val baseProperties = row.getStruct(3)
    val uid = baseProperties.getString(0)
    val eventType = baseProperties.getString(1)
    val version = baseProperties.getString(2)
    val ts = baseProperties.getLong(3)

    Event(itemId, quantity, price, uid, eventType, version, ts)
})(Encoders.product[Event])

newEventDs.printSchema()

newEventDs.show(5)



