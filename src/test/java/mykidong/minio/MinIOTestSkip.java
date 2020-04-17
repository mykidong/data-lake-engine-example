package mykidong.minio;

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

public class MinIOTestSkip {

    @Test
    public void run() throws Exception
    {
        SparkConf sparkConf = new SparkConf().setAppName("minio-test");
        sparkConf.setMaster("local[2]");

        sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse");
        sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();


        Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();

        // set hadoop configuration.

        // hadoop configuration.
        Resource resource = new ClassPathResource("hadoop-conf.properties");
        Properties hadoopProps = PropertiesLoaderUtils.loadProperties(resource);

        for (String key : hadoopProps.stringPropertyNames()) {
            String value = hadoopProps.getProperty(key);
            hadoopConfiguration.set(key, value);
        }

        // hive configuration.
        Properties hiveProps = PropertiesLoaderUtils.loadProperties(new ClassPathResource("hive-conf.properties"));
        for (String key : hiveProps.stringPropertyNames()) {
            String value = hiveProps.getProperty(key);
            hadoopConfiguration.set(key, value);
        }

        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load("/test-event-parquet");

        parquetDs.show(10);


        // change file system.
        hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
        hadoopConfiguration.set("fs.defaultFS", "s3a://mybucket.mc-d01.opasnet.io:9099");
        hadoopConfiguration.set("fs.s3a.endpoint", "http://mc-d01.opasnet.io:9099");
        hadoopConfiguration.set("fs.s3a.access.key", "minio");
        hadoopConfiguration.set("fs.s3a.secret.key", "minio123");
        hadoopConfiguration.set("fs.s3a.path.style.access", "true");
        hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // create persistent parquet table with external path.
        parquetDs.write().format("parquet")
                .option("path", "s3a://mybucket.mc-d01.opasnet.io:9099/test-minio")
                .mode(SaveMode.Overwrite)
                .save();


        System.out.println("reading from minio...");

        // read parquet from minio.
        Dataset<Row> dfFromMinio = spark.read().format("parquet")
                .load("s3a://mybucket.mc-d01.opasnet.io:9099/test-minio");

        dfFromMinio.show(10);
    }

}
