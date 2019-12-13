import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

// NOTE: variable 'spark' 가 이미 REPL 에 SparkSession instance 로 생성되었기 때문에
//       개발할때만 uncomment 하고 commit 할 경우는 comment 시킴.
//val spark: SparkSession

// TODO: 사용자 Request 별 Session 을 생성해야 하나....
// create new spark session.
val newSpark = spark.newSession

val jdbcDs = newSpark.read.format("jdbc")
  .option("url", "jdbc:mysql://mc-d01.opasnet.io:3306")
  .option("dbtable", "superset.logs")
  .option("user", "superset")
  .option("password", "supersetpass")
  .option("partitionColumn", "id")
  .option("lowerBound", "1")
  .option("upperBound", "100")
  .option("numPartitions", "10")
  .option("fetchsize", "100")
  .load

jdbcDs.show(100)

jdbcDs.printSchema()

println(s"row count: ${jdbcDs.count()}")

println(s"partition count: ${jdbcDs.rdd.partitions.length}")


jdbcDs.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("test.superset_logs")