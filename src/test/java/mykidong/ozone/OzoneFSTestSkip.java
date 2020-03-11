package mykidong.ozone;

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

import java.io.ByteArrayInputStream;
import java.util.Properties;

import static mykidong.util.StringUtils.fileToString;

public class OzoneFSTestSkip {

    @Test
    public void runOnOzoneFs() throws Exception
    {
        SparkConf sparkConf = new SparkConf().setAppName("ozone-fs-test");
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

        // ozone configuration.
        String ozoneConf = fileToString("ozone-site.xml");
        hadoopConfiguration.addResource(new ByteArrayInputStream(ozoneConf.getBytes()));

        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load("/test-event-parquet");

        parquetDs.show(10);

        // change file system.
        hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
        hadoopConfiguration.set("fs.defaultFS", "o3fs://mc-m01.opasnet.io:9862");
        hadoopConfiguration.set("fs.o3fs.impl", "org.apache.hadoop.fs.ozone.BasicOzoneFileSystem");
        hadoopConfiguration.set("fs.AbstractFileSystem.o3fs.impl", "org.apache.hadoop.fs.ozone.OzFs");

        // create persistent parquet table with external path.
        parquetDs.write().format("parquet")
                .option("path", "o3fs://my-bucket.my-volumne.mc-m01.opasnet.io:9862/test-ozone")
                .mode(SaveMode.Overwrite)
                .save();


        Dataset<Row> dfFromOzone = spark.read().format("parquet")
                .load("o3fs://my-bucket.my-volumne.mc-m01.opasnet.io:9862/test-ozone");

        System.out.println("reading from ozone file system...");

        dfFromOzone.show(10);
    }

}
