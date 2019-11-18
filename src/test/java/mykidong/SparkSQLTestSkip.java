package mykidong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.util.Properties;

public class SparkSQLTestSkip {

    @Test
    public void saveAsTable() throws Exception
    {
        // spark configuration for local mode.
        SparkConf sparkConf = new SparkConf().setAppName(SparkSQLTestSkip.class.getName());
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.sql.warehouse.dir", "/spark-hive-warehouse");
        sparkConf.set("hive.metastore.uris","thrift://mc-m01.opasnet.io:10016");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        // hadoop configuration.
        Resource resource = new ClassPathResource("hadoop-conf.properties");
        Properties hadoopProps = PropertiesLoaderUtils.loadProperties(resource);

        Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();

        // set hadoop configuration.
        for (String key : hadoopProps.stringPropertyNames()) {
            String value = hadoopProps.getProperty(key);
            hadoopConfiguration.set(key, value);
        }

        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load("/test-event-parquet");

        // create persistent parquet table with external path.
        parquetDs.write().format("parquet")
                .option("path", "hdfs://mc/test-event-parquet-table")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test_parquet_table");


    }
}
