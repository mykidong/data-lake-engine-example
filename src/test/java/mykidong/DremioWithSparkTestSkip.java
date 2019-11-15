package mykidong;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.util.Properties;

public class DremioWithSparkTestSkip {

    @Test
    public void processDataFromDremio() throws Exception
    {
        String tableName = System.getProperty("tableName", "\"mc-hive\".\"default\".\"student\"");

        // spark configuration for local mode.
        SparkConf sparkConf = new SparkConf().setAppName(DremioWithSparkTestSkip.class.getName());
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.sql.parquet.compression.codec", "snappy");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        // hadoop configuration.
        Resource resource = new ClassPathResource("hadoop-conf.properties");
        Properties hadoopProps = PropertiesLoaderUtils.loadProperties(resource);

        Configuration hadoopConfiguration = sparkSession.sparkContext().hadoopConfiguration();

        // set hadoop configuration.
        for (String key : hadoopProps.stringPropertyNames()) {
            String value = hadoopProps.getProperty(key);
            hadoopConfiguration.set(key, value);
        }

        Class.forName("com.dremio.jdbc.Driver");

        // URL parameters
        String url = "jdbc:dremio:direct=mc-d01.opasnet.io:31010";

        Properties properties = new Properties();
        properties.setProperty("user", "mykidong");
        properties.setProperty("password", "icarus0337");

        Dataset<Row> jdbcDs = sparkSession.read()
                .jdbc(url, tableName, properties);

        System.out.println("size: " + jdbcDs.count());

        for(Row row : jdbcDs.collectAsList())
        {
            System.out.println("row: " + row.toString());
        }
    }
}
