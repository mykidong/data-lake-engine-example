package mykidong;

import org.apache.hadoop.conf.Configuration;
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
        sparkConf.set("spark.sql.hive.metastore.version", "3.1.0");
        sparkConf.set("hive.metastore.uris","thrift://mc-d01.opasnet.io:9083,thrift://mc-d02.opasnet.io:9083");

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
                .load("hdfs://mc/test-event-parquet");

        // create persistent parquet table with external path.
        parquetDs.write().format("parquet")
                .option("path", "hdfs://mc/test-event-parquet-table")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test_parquet_table");


    }
}
