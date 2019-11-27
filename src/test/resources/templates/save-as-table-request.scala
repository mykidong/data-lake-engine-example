import org.apache.spark.sql.{SaveMode, SparkSession}

class SparkRunner extends mykidong.reflect.DynamicScalaSparkJobRunner {
    override def run(spark: SparkSession): String = {
        // read parquet.
        val parquetDs = spark.read.format("parquet")
                .load("/test-event-parquet")

        // create persistent parquet table with external path.
        parquetDs.write.format("parquet")
                .option("path", "hdfs://mc/test-event-parquet-table")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test_parquet_dynamic_request")
        "SUCCESS!!"
    }
}
scala.reflect.classTag[SparkRunner].runtimeClass