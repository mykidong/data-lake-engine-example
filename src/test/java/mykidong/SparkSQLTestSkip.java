package mykidong;

import com.cedarsoftware.util.io.JsonWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import mykidong.reflect.DynamicSparkRunner;
import mykidong.util.Log4jConfigurer;
import mykidong.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.joor.Reflect;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class SparkSQLTestSkip {

    private static Logger log = LoggerFactory.getLogger(SparkSQLTestSkip.class);

    private SparkSession spark;

    @Before
    public void init() throws Exception
    {
        // init. log4j.
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j.xml");
        log4j.afterPropertiesSet();


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



    /**
     * Since hive is not case-sensitive for column names, Spark is case-sensitive!!!
     *
     * @throws Exception
     */
    @Test
    public void createHiveTable() throws Exception
    {
        String path = "/test-event-parquet";

        String query = "";
        query += "CREATE EXTERNAL TABLE IF NOT EXISTS test.event (";
        query += "   itemId          STRING,";
        query += "    quantity        BIGINT,";
        query += "    price           BIGINT,";
        query += "    baseProperties   STRUCT<uid:             STRING,";
        query += "                            eventType:       STRING,";
        query += "                            version:         STRING,";
        query += "                            ts:              BIGINT>";

        query += ")    ";
        query += "STORED AS PARQUET   ";
        query += "LOCATION 'hdfs://mc" + path + "'";

        spark.sql(query);
    }


    /**
     * Since hive is not case-sensitive for column names, Spark is case-sensitive!!!
     *
     * @throws Exception
     */
    @Test
    public void readFromPersistentTable() throws Exception
    {
        spark.sql("select * from test_parquet_table limit 10").show();

        spark.sql("select itemId, baseProperties.ts from test_parquet_table").show();
    }


    @Test
    public void readFromMySQLAndSaveAsTable() throws Exception
    {
        Dataset<Row> jdbcDs = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://mc-m02.opasnet.io:3306")
                .option("dbtable", "azkaban.projects")
                .option("user", "azkaban")
                .option("password", "azkabanpass")
                .load();

        jdbcDs.show(10);

        jdbcDs.printSchema();
        StructType schema = jdbcDs.schema();

        jdbcDs.write().format("parquet")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test.azkaban_projects");
    }


    /**
     * Since hive is not case-sensitive for column names, Spark is case-sensitive!!!
     *
     * @throws Exception
     */
    @Test
    public void createHiveTableWithoutCopyingFile() throws Exception
    {
        String path = "/test-event-parquet";

        String tableName = "test.without_copying_file";

        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load(path);

        String ddl = parquetDs.schema().toDDL();
        log.info("ddl: [" + ddl + "]");

        String query = "";
        query += "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + " (";
        query += ddl;
        query += ")    ";
        query += "STORED AS PARQUET   ";
        query += "LOCATION 'hdfs://mc" + path + "'";

        spark.sql("drop table if exists " + tableName);
        spark.sql(query);
    }


    /**
     * Since hive is not case-sensitive for column names, Spark is case-sensitive!!!
     *
     * @throws Exception
     */
    @Test
    public void readSchemaFromHiveAndCreateNewHiveTable() throws Exception
    {
        String tableName = "test.without_copying_file";

        // spark hive metastore for hive 1.2.x.
        String url = "jdbc:hive2://mc-m01.opasnet.io:10016";

        Properties properties = new Properties();
        properties.put("user", "hive");

        HiveJdbcMetadata hiveJdbcMetadata = new HiveJdbcMetadata(url, properties);
        HiveJdbcMetadata.HiveMetadataMap hiveMetadataMap = hiveJdbcMetadata.getMetadataFromHive(tableName);

        Map<String, String> ddlMap = hiveMetadataMap.getDdlMap();
        Map<String, String> extraInfoMap = hiveMetadataMap.getExtraInfoMap();

        log.info("ddl: [" + JsonWriter.formatJson(new ObjectMapper().writeValueAsString(ddlMap)) + "]");
        log.info("extra: [" + JsonWriter.formatJson(new ObjectMapper().writeValueAsString(extraInfoMap)) + "]");

        String ddl = "";
        int count = 0;
        for(String columnName : ddlMap.keySet())
        {
            if(count > 0)
            {
                ddl += "," + columnName + " " + ddlMap.get(columnName);
            }
            else
            {
                ddl += columnName + " " + ddlMap.get(columnName);
            }
            count++;
        }

        String location = extraInfoMap.get("Location");

        String newTableName = "test.without_copying_file_new";

        String query = "";
        query += "CREATE EXTERNAL TABLE IF NOT EXISTS " + newTableName + " (";
        query += ddl;
        query += ")    ";
        query += "STORED AS PARQUET   ";
        query += "LOCATION '" + location + "'";

        spark.sql("drop table if exists " + newTableName);
        spark.sql(query);
    }


    /**
     * Since hive is not case-sensitive for column names, Spark is case-sensitive!!!
     *
     * @throws Exception
     */
    @Test
    public void createNewHiveTableForExternalHiveMetastore() throws Exception
    {
        String tableName = "another_test.new_event";

        // hive metastore 3.x.
        String url = "jdbc:hive2://mc-d01.opasnet.io:10000";

        Properties properties = new Properties();
        properties.put("user", "hive");

        HiveJdbcMetadata hiveJdbcMetadata = new HiveJdbcMetadata(url, properties);
        HiveJdbcMetadata.HiveMetadataMap hiveMetadataMap = hiveJdbcMetadata.getMetadataFromHive(tableName);

        Map<String, String> ddlMap = hiveMetadataMap.getDdlMap();
        Map<String, String> extraInfoMap = hiveMetadataMap.getExtraInfoMap();

        log.info("ddl: [" + JsonWriter.formatJson(new ObjectMapper().writeValueAsString(ddlMap)) + "]");
        log.info("extra: [" + JsonWriter.formatJson(new ObjectMapper().writeValueAsString(extraInfoMap)) + "]");

        // first, read parquet schema from the location path to get DDL.

        // hdfs path.
        String location = extraInfoMap.get("Location");

        // read parquet.
        Dataset<Row> parquetDs = spark.read().format("parquet")
                .load(location);

        String ddlWithCaseSensitive = parquetDs.schema().toDDL();
        log.info("ddlWithCaseSensitive: [" + ddlWithCaseSensitive + "]");

        String newTableName = "test.new_event_from_another";

        String query = "";
        query += "CREATE EXTERNAL TABLE IF NOT EXISTS " + newTableName + " (";
        query += ddlWithCaseSensitive;
        query += ")    ";
        query += "STORED AS PARQUET   ";
        query += "LOCATION '" + location + "'";

        log.info("create table sql: [" + query + "]");

        spark.sql("drop table if exists " + newTableName);
        spark.sql(query);
    }


    /**
     * JDBC Without Copy DOES NOT WORK!!
     *
     * Error: org.apache.spark.sql.catalyst.parser.ParseException:
     * Operation not allowed: STORED BY(line 13, pos 0)
     *
     * @throws Exception
     */
    @Test
    public void createHiveTableForExternalDB() throws Exception
    {
        String url = "jdbc:mysql://mc-m02.opasnet.io:3306";
        String user = "azkaban";
        String password = "azkabanpass";
        String table = "azkaban.projects";

        Dataset<Row> jdbcDs = spark.read().format("jdbc")
                .option("url", url)
                .option("user", user)
                .option("password", password)
                .option("query", "select * from " + table + " limit 1")
                .load();

        jdbcDs.show();

        String ddlWithCaseSensitive = jdbcDs.schema().toDDL();
        log.info("ddlWithCaseSensitive: [" + ddlWithCaseSensitive + "]");

        String newTable = "test.azkaban_without_copy";

        String query = "";
        query += "CREATE EXTERNAL TABLE " + newTable;
        query += "(";
        query += ddlWithCaseSensitive;
        query += ")   ";
        query += "STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler' ";
        query += "TBLPROPERTIES (";
        query += "        \"hive.sql.database.type\" = \"MYSQL\",";
        query += "        \"hive.sql.jdbc.driver\" = \"com.mysql.jdbc.Driver\",";
        query += "        \"hive.sql.jdbc.url\" = \"" + url + "\",";
        query += "        \"hive.sql.dbcp.username\" = \"" + user + "\",";
        query += "        \"hive.sql.dbcp.password\" = \"" + password + "\",";
        query += "        \"hive.sql.table\" = \"" + table + "\",";
        query += "        \"hive.sql.dbcp.maxActive\" = \"1\"";
        query += ")";

        log.info("create table sql: [" + query + "]");

        spark.sql("drop table if exists " + newTable);
        spark.sql(query);
    }

    @Test
    public void runDynamicCompiledSparkCodes() throws Exception
    {
        // read java codes.
        String codes = StringUtils.fileToString("/templates/save-as-table.java");

        DynamicSparkRunner sparkRunner = Reflect.compile(
                "mykidong.SparkRunner", codes).create().get();

        // run spark codes dynamically.
        sparkRunner.run(spark);
    }
}
