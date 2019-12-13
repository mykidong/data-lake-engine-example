import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.types.StructType

val jdbcDs = spark.read.format("jdbc")
  .option("url", "jdbc:mysql://mc-d01.opasnet.io:3306")
  .option("dbtable", "superset.logs")
  .option("user", "superset")
  .option("password", "supersetpass")
  .option("partitionColumn", "id")
  .option("lowerBound", "1")
  .option("upperBound", "100")
  .option("numPartitions", "10")
  .load

jdbcDs.show(100)

jdbcDs.printSchema()
val schema = jdbcDs.schema

jdbcDs.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("test.superset_logs")