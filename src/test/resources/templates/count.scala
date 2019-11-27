import mykidong.reflect.DynamicScalaSparkJobRunner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}

class CountRunner extends DynamicScalaSparkJobRunner{
    override def run(spark: SparkSession): String = {
        val parquetDs = spark.read.format("parquet")
          .load("/test-event-parquet")

        parquetDs.show(3)

        implicit val intEncoder = Encoders.INT
        val sum = parquetDs.map(row => {
            println("row: " + row.toString)

            1
        }).count()

        "sum: " + sum
    }
}
scala.reflect.classTag[CountRunner].runtimeClass