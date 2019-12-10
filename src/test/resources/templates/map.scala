import org.apache.spark.sql.{Dataset, Row, RowFactory, SaveMode, SparkSession}

//val spark: SparkSession

val parquetDs = spark.read.format("parquet")
  .load("/test-event-parquet")

parquetDs.show(5)

import org.apache.spark.sql.Encoders

val newEventRdd = parquetDs.map(row => {
    val itemId = row.get(0)
    val quantity = row.get(1)
    val price = row.get(2)
    val baseProperties = row.getStruct(3)
    val uid = baseProperties.getString(0)
    val eventType = baseProperties.getString(1)
    val version = baseProperties.getString(2)
    val ts = baseProperties.getLong(3)

    RowFactory.create(itemId, quantity, price, uid, eventType, version, ts)
})(Encoders.kryo[Row])

newEventRdd.printSchema()

newEventRdd.show(5)



