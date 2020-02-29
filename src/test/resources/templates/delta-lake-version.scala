import org.apache.spark.sql.{SaveMode, SparkSession}
import io.delta.tables._


// NOTE: variable 'spark' 가 이미 REPL 에 SparkSession instance 로 생성되었기 때문에
//       개발할때만 uncomment 하고 commit 할 경우는 comment 시킴.
//val spark: SparkSession


// TODO: 개발 현재 delta laks 는 Spark 2.4.2 가 필요함으로 지금의 Spark Cluster(2.3.2) 에는 적용을 할수 없음.

// TODO: 사용자 Request 별 Session 을 생성해야 하나....
// create new spark session.
val newSpark = spark.newSession

val deltaTable = DeltaTable.forPath(newSpark, "/test-delta-table")
println("show version history...")
deltaTable.history.show(false)

println("reading delta table with version 63...")
val version63 = spark.read.format("delta").option("versionAsOf", 63).load("/test-delta-table")
version63.show(200)
version63.count


println("reading delta table with version 62...")
val version62 = spark.read.format("delta").option("versionAsOf", 62).load("/test-delta-table")
version62.show(200)
version62.count


// set result data frame to getBack var.
getBack.setResult(deltaTable.toDF)


