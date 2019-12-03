import mykidong.reflect.DynamicScalaSparkJobRunner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}

trait DynamicScalaSparkJobRunner {
    def run(spark: SparkSession): String
}

class CountRunner extends DynamicScalaSparkJobRunner{
    override def run(spark: SparkSession): String = {

        val lines = sc.textFile("/crawl/text/year=2019/month=12/day=02/hour=09/gyeonggido_category.json")
        val lineLengths = lines.map(s => s.length)
        val totalLength = lineLengths.reduce((a, b) => a + b)
        println("totalLength: [" + totalLength + "]");


        "sum: " + totalLength
    }
}
scala.reflect.classTag[CountRunner].runtimeClass
val runner = new CountRunner()
runner.run(spark)