package mykidong.ceph;

import mykidong.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.actors.threadpool.Arrays;

public class CephTestSkip {

    @Test
    public void run() throws Exception
    {
        SparkConf sparkConf = new SparkConf().setAppName("ceph-test");
        sparkConf.setMaster("local[2]");
        
        // delta lake log store for s3.
        sparkConf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();


        Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();

        // set hadoop configuration.
        hadoopConfiguration.set("fs.defaultFS", "s3a://mykidong-bucket");
        hadoopConfiguration.set("fs.s3a.endpoint", "http://10.1.14.8:7480"); // MUST IP Address(Not Host Name) !!!!
        hadoopConfiguration.set("fs.s3a.access.key", "G0XLFLAX60PIXB1RTOT7");
        hadoopConfiguration.set("fs.s3a.secret.key", "FvvVpgx3qh5LqduDeIfFg27w6uWzE3fl4qtjXjvz");
        hadoopConfiguration.set("fs.s3a.path.style.access", "true");
        hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // read json.
        String json = StringUtils.fileToString("data/test.json");
        String lines[] = json.split("\\r?\\n");
        Dataset<Row> df = spark.read().json(new JavaSparkContext(spark.sparkContext()).parallelize(Arrays.asList(lines)));

        // write delta to ceph.
        df.write().format("delta")
                .option("path", "s3a://mykidong-bucket/test-delta")
                .mode(SaveMode.Overwrite)
                .save();

        // read delta from ceph.
        Dataset<Row> deltaFromCeph = spark.read().format("delta")
                .load("s3a://mykidong-bucket/test-delta");

        deltaFromCeph.show(10);
    }

}
