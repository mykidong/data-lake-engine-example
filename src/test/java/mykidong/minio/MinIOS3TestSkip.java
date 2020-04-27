package mykidong.minio;

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

public class MinIOS3TestSkip {

    @Test
    public void run() throws Exception
    {
        SparkConf sparkConf = new SparkConf().setAppName("minio-s3-test");
        sparkConf.setMaster("local[2]");
        
        // delta lake log store for s3.
        sparkConf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();


        Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();

        // set hadoop configuration.
        hadoopConfiguration.set("fs.defaultFS", "s3a://ozone-bucket");
        hadoopConfiguration.set("fs.s3a.endpoint", "http://mc-m01.opasnet.io:9878"); // MUST IP Address(Not Host Name) !!!!
        hadoopConfiguration.set("fs.s3a.access.key", "mykidong/mc-d02.opasnet.io@OPASNET.IO");
        hadoopConfiguration.set("fs.s3a.secret.key", "3b38bbbc22f8c111269b9d3a04a9379a661e10217af332e7e8185f35949983aa");
        hadoopConfiguration.set("fs.s3a.path.style.access", "true");
        hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // read json.
        String json = StringUtils.fileToString("data/test.json");
        String lines[] = json.split("\\r?\\n");
        Dataset<Row> df = spark.read().json(new JavaSparkContext(spark.sparkContext()).parallelize(Arrays.asList(lines)));

        // write parquet to ozone.
        df.write().format("delta")
                .option("path", "s3a://ozone-bucket/test-delta")
                .mode(SaveMode.Overwrite)
                .save();

        // read delta from ozone.
        Dataset<Row> deltaFromCeph = spark.read().format("delta")
                .load("s3a://ozone-bucket/test-delta");

        deltaFromCeph.show(10);
    }

}
