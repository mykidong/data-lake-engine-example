package mykidong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.util.Properties;

public class SparkSQLTestSkip {

    private SparkSession spark;

    @Before
    public void init() throws Exception
    {
        // spark configuration for local mode.
        SparkConf sparkConf = new SparkConf().setAppName(SparkSQLTestSkip.class.getName());
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
    public void saveAsTable() throws Exception
    {
        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load("/test-event-parquet");

        // create persistent parquet table with external path.
        parquetDs.write().format("parquet")
                .option("path", "hdfs://mc/test-event-parquet-table")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test_parquet_table");


    }

    @Test
    public void saveAsTableWithoutPath() throws Exception
    {
        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load("/test-event-parquet");

        // create persistent parquet table.
        // file location: hdfs://mc-m01.opasnet.io:8020/apps/spark/warehouse/test_parquet_table2
        parquetDs.write().format("parquet")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test_parquet_table2");
    }


    @Test
    public void createDatabase() throws Exception
    {
        String database = "test";

        spark.sql("CREATE DATABASE IF NOT EXISTS " + database);
    }


    @Test
    public void saveAsTableWithoutPathInDatabase() throws Exception
    {
        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load("/test-event-parquet");

        // create persistent parquet table in a db.
        // file location: hdfs://mc/spark-warehouse/test.db/test_parquet_table_in_db
        parquetDs.write().format("parquet")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test.test_parquet_table_in_db");
    }



    @Test
    public void createHiveTable() throws Exception
    {
        String path = "/test-event-parquet";

        String query = "";
        query += "CREATE EXTERNAL TABLE IF NOT EXISTS test.event (";
        query += "   itemId          STRING,";
        query += "    quantity        INT,";
        query += "    price           BIGINT,";
        query += "    baseProperties   STRUCT<uid:             STRING,";
        query += "                            eventType:       STRING,";
        query += "                            version:         STRING,";
        query += "                            ts:              BIGINT>";

        query += ")  ";
        query += "STORED AS PARQUET   ";
        query += "LOCATION 'hdfs://mc" + path + "'";

        spark.sql(query);
    }


    @Test
    public void readFromPersistentTable() throws Exception
    {
        spark.sql("select * from test_parquet_table limit 10").show();

        spark.sql("select itemId, baseProperties.ts from test_parquet_table").show();
    }
}
