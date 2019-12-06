import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.types.StructType

val jdbcDs = spark.read.format("jdbc")
  .option("url", "jdbc:mysql://mc-m02.opasnet.io:3306")
  .option("dbtable", "azkaban.projects")
  .option("user", "azkaban")
  .option("password", "azkabanpass")
  .load

jdbcDs.show(10)

jdbcDs.printSchema()
val schema = jdbcDs.schema

jdbcDs.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("test.azkaban_projects")