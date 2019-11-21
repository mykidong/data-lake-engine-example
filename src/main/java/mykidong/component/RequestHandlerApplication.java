package mykidong.component;

import mykidong.http.RequestHandlerHttpServer;
import mykidong.util.Log4jConfigurer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandlerApplication {

    private static Logger log = LoggerFactory.getLogger(RequestHandlerApplication.class);

    public static void main(String[] args)
    {
        // init. log4j.
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j.xml");
        try {
            log4j.afterPropertiesSet();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        // spark configuration.
        SparkConf sparkConf = new SparkConf().setAppName(RequestHandlerApplication.class.getName());
        sparkConf.set("spark.sql.warehouse.dir", "hdfs://mc/spark-warehouse");
        sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/3.1.4.0-315/spark2/standalone-metastore/standalone-metastore-1.21.2.3.1.4.0-315-hive3.jar");

        // spark session.
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        // run embeded http server.
        int port = 8125;
        RequestHandlerHttpServer httpServer = new RequestHandlerHttpServer(port, spark);
        log.info("embed http server is running now....");


        spark.stop();
    }
}
