import mykidong.reflect.DynamicScalaSparkJobRunner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}

trait DynamicScalaSparkJobRunner {
    def run(spark: SparkSession): String
}

class CountRunner extends DynamicScalaSparkJobRunner{
    override def run(spark: SparkSession): String = {

        val parquetDs = spark.read.format("parquet")
          .load("/test-event-parquet")

        parquetDs.show(3)

        implicit val intEncoder = Encoders.scalaInt
        val sum = parquetDs.map(row => {
            println("row: " + row.toString);
            return 1;
        }).count()

        "sum: " + sum
    }
}
scala.reflect.classTag[CountRunner].runtimeClass
val runner = new CountRunner()
runner.run(spark)