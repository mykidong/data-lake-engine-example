import mykidong.reflect.DynamicScalaSparkJobRunner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}

trait DynamicScalaSparkJobRunner {
    def run(spark: SparkSession): String
}

class CountRunner extends DynamicScalaSparkJobRunner{
    override def run(spark: SparkSession): String = {

        spark.sql("select * from test_parquet_table limit 10").show()

        spark.sql("select itemId, baseProperties.ts from test_parquet_table").show()


        "sum: "
    }
}
scala.reflect.classTag[CountRunner].runtimeClass
val runner = new CountRunner()
runner.run(spark)