import org.apache.spark.sql.SparkSession

/**
 * 이 Codes 는 DynamicCompileSpec Test Class 를 위함.
 */
class CountRunner extends mykidong.reflect.DynamicScalaSparkJobRunner {
  override def run(spark: SparkSession): String = {

    val data = Array(1, 2, 3, 4, 5)

    // spark.sparkContext 때문에 Error:
    // illegal cyclic reference involving object InterfaceAudience
    val distData = spark.sparkContext.parallelize(data)

    val sum = distData.reduce((a, b) => a + b)

    "sum: " + sum
  }
}
scala.reflect.classTag[CountRunner].runtimeClass