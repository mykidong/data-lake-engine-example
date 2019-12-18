package mykidong.sftp;

import mykidong.util.Log4jConfigurer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.util.Properties;

public class SftpDataSourceTestSkip {

    private static Logger log = LoggerFactory.getLogger(SftpDataSourceTestSkip.class);

    private SparkSession spark;

    @Before
    public void init() throws Exception
    {
        // init. log4j.
        Log4jConfigurer.loadLog4j(null);


        // spark configuration for local mode.
        SparkConf sparkConf = new SparkConf().setAppName(SftpDataSourceTestSkip.class.getName());
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse");
        sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar");

        spark = SparkSession
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
    }

    @Test
    public void downloadAndUploadCsv() throws Exception
    {
        Dataset<Row> df = spark.read().
                format("com.springml.spark.sftp").
                option("host", "mc-d03.opasnet.io").
                option("username", "mysftpuser").
                option("password", "icarus").
                option("fileType", "csv").
                option("delimiter", "|").
                option("quote", "\"").
                option("escape", "\\").
                option("multiLine", "false").
                option("inferSchema", "true").
                load("test.csv");

        df.printSchema();
        df.show(3);


        // upload file to sftp.
        df.write().
                format("com.springml.spark.sftp").
                option("host", "mc-d03.opasnet.io").
                option("username", "mysftpuser").
                option("password", "icarus").
                option("fileType", "csv").
                mode(SaveMode.Overwrite).
                save("uploaded-test.csv");
    }
}
