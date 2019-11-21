package mykidong.reflect;

import org.apache.spark.sql.SparkSession;

public interface DynamicSparkRunner {

    public void run(SparkSession spark) throws Exception;
}
