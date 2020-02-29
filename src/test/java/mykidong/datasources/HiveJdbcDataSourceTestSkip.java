package mykidong.datasources;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.util.Properties;

public class HiveJdbcDataSourceTestSkip {

    @Test
    public void loadDataFromHive() throws Exception
    {
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("windows")) {
            System.setProperty("hadoop.home.dir", "C:\\hadoop-home");
        }

        SparkConf sparkConf = new SparkConf().setAppName("traverse schema");
        sparkConf.setMaster("local[2]");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        // hadoop configuration.
        Resource resource = new ClassPathResource("hadoop-conf.properties");
        Properties hadoopProps = PropertiesLoaderUtils.loadProperties(resource);

        // set hadoop configuration to the current spark session.
        spark = SparkLoader.getSessionWithHadoopProperties(spark, hadoopProps);

        Dataset<Row> jdbcHiveDf = spark.read().format("jdbc-hive")
                .option("dbTable", "mc.crawl_youtube")
                .option("conditionClause", "limit 100")
                .option("hiveJdbcUrl", "jdbc:hive2://mc-d01.opasnet.io:10000")
                .option("hiveJdbcUser", "hdfs")
                .option("hiveJdbcPassword", "hdfspass")
                .option("hiveMetastoreUrl", "jdbc:mysql://mc-d01.opasnet.io:3306/hive")
                .option("hiveMetastoreUser", "hdfs")
                .option("hiveMetastorePassword", "hdfspass")
                .option("fetchsize", "10")
                .option("tempPath", "/jdbc-hive-temp")
                .load();

        jdbcHiveDf.show(100);

        jdbcHiveDf.printSchema();
    }
}
