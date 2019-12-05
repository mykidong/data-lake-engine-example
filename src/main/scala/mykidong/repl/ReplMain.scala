package mykidong.repl

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.Locale

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import scala.tools.nsc.GenericRunnerSettings


object ReplMain extends Logging {

  initializeLogIfNecessary(true)

  val conf = new SparkConf()

  val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
  val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  // this is a public var because tests reset it.
  var interp: ReplExec = _

  private var hasErrors = false
  private var isShellSession = false

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    // scalastyle:off println
    Console.err.println(msg)
    // scalastyle:on println
  }

  def main(args: Array[String]): Unit = {
    isShellSession = true
    doMain(args, new ReplExec)
  }

  def getLocalUserJarsForShell(conf: SparkConf): Seq[String] = {
    val localJars = conf.getOption("spark.repl.local.jars")
    localJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
  }

  // Visible for testing
  private[repl] def doMain(args: Array[String], _interp: ReplExec): Unit = {
    interp = _interp

    conf.set("spark.repl.local.jars", "/home/mc/data-lake-engine-example/target/data-lake-example-1.0.0-SNAPSHOT-spark-job.jar")

    val jars = getLocalUserJarsForShell(conf)
      // Remove file:///, file:// or file:/ scheme if exists for each jar
      .map { x => if (x.startsWith("file:")) new File(new URI(x)).getPath else x }
      .mkString(File.pathSeparator)
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", jars
    ) ++ args.toList

    val settings = new GenericRunnerSettings(scalaOptionError)
    settings.processArguments(interpArguments, true)

    if (!hasErrors) {
      interp.process(settings) // Repl starts and goes in loop of R.E.P.L
      Option(sparkContext).foreach(_.stop)
    }
  }

  def createSparkSession(): SparkSession = {
    try {
      val execUri = System.getenv("SPARK_EXECUTOR_URI")
      conf.setIfMissing("spark.app.name", "Spark shell")
      // SparkContext will detect this configuration and register it with the RpcEnv's
      // file server, setting spark.repl.class.uri to the actual URI for executors to
      // use. This is sort of ugly but since executors are started as part of SparkContext
      // initialization in certain cases, there's an initialization order issue that prevents
      // this from being set after SparkContext is instantiated.
      conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath())
      if (execUri != null) {
        conf.set("spark.executor.uri", execUri)
      }
      if (System.getenv("SPARK_HOME") != null) {
        conf.setSparkHome(System.getenv("SPARK_HOME"))
      }

      val builder = SparkSession.builder.config(conf)
      if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase(Locale.ROOT) == "hive") {
        sparkSession = builder.enableHiveSupport().getOrCreate()
        logInfo("Created Spark session with Hive support")
      } else {
        // In the case that the property is set but not to 'hive', the internal
        // default is 'in-memory'. So the sparkSession will use in-memory catalog.
        sparkSession = builder.getOrCreate()
        logInfo("Created Spark session")
      }
      sparkContext = sparkSession.sparkContext
      sparkSession
    } catch {
      case e: Exception if isShellSession =>
        logError("Failed to initialize Spark session.", e)
        sys.exit(1)
    }
  }

}
