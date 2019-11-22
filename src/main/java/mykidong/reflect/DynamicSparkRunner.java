package mykidong.reflect;

import org.apache.spark.sql.SparkSession;

public interface DynamicSparkRunner {

    public String run(SparkSession spark) throws Exception;
}
