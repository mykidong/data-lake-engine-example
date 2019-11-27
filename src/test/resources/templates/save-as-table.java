package mykidong;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

class SparkRunner implements mykidong.reflect.DynamicSparkRunner {
    public void run(SparkSession spark) throws Exception {
        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load("/test-event-parquet");

        // create persistent parquet table with external path.
        parquetDs.write().format("parquet")
                .option("path", "hdfs://mc/test-event-parquet-table")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test_parquet_dynamic");
    }
}