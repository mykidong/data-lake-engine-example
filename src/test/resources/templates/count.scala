import mykidong.reflect.DynamicScalaSparkJobRunner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}

trait DynamicScalaSparkJobRunner {
    def run(spark: SparkSession): String
}

class CountRunner extends DynamicScalaSparkJobRunner{
    override def run(spark: SparkSession): String = {

        val data = Array(1, 2, 3, 4, 5)
        val distData = sc.parallelize(data)

        val sum = distData.reduce( (a, b) => a + b)


        "sum: " + sum
    }
}
scala.reflect.classTag[CountRunner].runtimeClass
val runner = new CountRunner()
runner.run(spark)