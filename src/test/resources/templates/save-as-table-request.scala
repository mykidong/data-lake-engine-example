import org.apache.spark.sql.{SaveMode, SparkSession}

trait DynamicScalaSparkJobRunner {
    def run(spark: SparkSession): String
}

class SparkRunner extends DynamicScalaSparkJobRunner {
    override def run(spark: SparkSession): String = {
        // read parquet.
        val parquetDs = spark.read.format("parquet")
                .load("/test-event-parquet")

        parquetDs.show(5)

        // create persistent parquet table with external path.
        parquetDs.write.format("parquet")
                .option("path", "hdfs://mc/test-event-parquet-table")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test_parquet_dynamic_request")
        "SUCCESS!!"
    }
}
scala.reflect.classTag[SparkRunner].runtimeClass

val runner = new SparkRunner()
runner.run(spark)