package mykidong.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.actors.threadpool.Arrays;

import java.util.List;

class CountRunner implements mykidong.reflect.DynamicSparkRunner {
    public String run(SparkSession spark) throws Exception {

        List<Integer> intList = Arrays.asList(new Integer[]{1, 2, 3, 4, 5, 6, 7, 10, 11});

        JavaRDD<Integer> rdd = new JavaSparkContext(spark.sparkContext()).parallelize(intList);
        int sum = rdd.reduce((a, b) -> a + b);

        return "sum: " + sum;
    }
}