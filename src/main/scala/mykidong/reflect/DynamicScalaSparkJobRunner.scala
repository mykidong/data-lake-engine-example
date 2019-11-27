package mykidong.reflect

import org.apache.spark.sql.SparkSession

trait DynamicScalaSparkJobRunner {

  def run(spark: SparkSession): String

}
