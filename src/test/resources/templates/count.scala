import org.apache.spark.sql.SparkSession

class CountRunner extends mykidong.reflect.DynamicScalaSparkJobRunner {
    override def run(spark: SparkSession): String = {

        val data = Array(1, 2, 3, 4, 5)
        val distData = spark.sparkContext.parallelize(data)

        val sum = distData.reduce((a, b) => a + b)

        "sum: " + sum
    }
}
scala.reflect.classTag[CountRunner].runtimeClass