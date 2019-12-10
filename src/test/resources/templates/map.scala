import org.apache.spark.sql.{Dataset, Row, RowFactory, SaveMode, SparkSession}

//val spark: SparkSession

val parquetDs = spark.read.format("parquet")
  .load("/test-event-parquet")

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



