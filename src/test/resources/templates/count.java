package mykidong.spark;

import com.cedarsoftware.util.io.JsonWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import mykidong.reflect.DynamicSparkRunner;
import mykidong.util.Log4jConfigurer;
import mykidong.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.joor.Reflect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import scala.actors.threadpool.Arrays;

import java.util.List;

class CountRunner implements mykidong.reflect.DynamicSparkRunner {
    public String run(SparkSession spark) throws Exception {

        List<Integer> intList = Arrays.asList(new Integer[]{1, 2, 3, 4, 5, 6, 7, 10, 11});

        JavaRDD<Integer> rdd = new JavaSparkContext(spark.sparkContext()).parallelize(intList);
        int sum = rdd.reduce(new Sum());

        return "sum: " + sum;
    }

    public static class Sum implements Function2<Integer, Integer, Integer>
    {
        @Override
        public Integer call(Integer a, Integer b) throws Exception {
            return a + b;
        }
    }
}