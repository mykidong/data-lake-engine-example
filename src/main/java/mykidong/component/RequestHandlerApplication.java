package mykidong.component;

import mykidong.http.RequestHandlerHttpServer;
import mykidong.util.Log4jConfigurer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandlerApplication {

    private static Logger log = LoggerFactory.getLogger(RequestHandlerApplication.class);

    public static void main(String[] args)
    {
        // init. log4j.
        Log4jConfigurer.loadLog4j(null);

        // spark configuration.
        SparkConf sparkConf = new SparkConf().setAppName(RequestHandlerApplication.class.getName());
        sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse");
        sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar");
        sparkConf.set("spark.dynamicAllocation.enabled", "true");
        sparkConf.set("spark.dynamicAllocation.minExecutors", "3");
        sparkConf.set("spark.dynamicAllocation.maxExecutors", "6");
        sparkConf.set("spark.shuffle.service.enabled", "true");


        // set fair scheduler mode.
        sparkConf.set("spark.scheduler.mode", "FAIR");
        sparkConf.set("spark.scheduler.allocation.file", "/usr/lib/mc/conf/fairscheduler.xml");

        // spark session.
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // run embeded http server.
        int port = 8125;
        RequestHandlerHttpServer httpServer = new RequestHandlerHttpServer(port, jsc, spark);
        try {
            httpServer.start();
            log.info("embed http server is running now....");

            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e)
        {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }

        spark.stop();
    }
}
