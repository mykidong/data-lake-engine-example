package mykidong.repl


import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.tools.nsc.GenericRunnerSettings


object ReplMain extends Logging {

  var rootDir: String = _
  var outputDir: File = _

  var conf: SparkConf = _
  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _

  // this is a public var because tests reset it.
  var interp: ReplExec = _

  private var hasErrors = false
  private var isShellSession = false

  def loadRepl(spark: SparkSession): Unit = {
    isShellSession = true
    initializeLogIfNecessary(true)

    conf = spark.sparkContext.getConf
    sparkSession = spark
    sparkContext = spark.sparkContext

    rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()

    spark.conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)
    log.info("spark.repl.class.outputDir: [" + outputDir.getAbsolutePath + "]");

    doMain(new ReplExec)
  }

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    // scalastyle:off println
    Console.err.println(msg)
    // scalastyle:on println
  }

  private def getLocalUserJarsForShell(conf: SparkConf): Seq[String] = {
    val localJars = conf.getOption("spark.repl.local.jars")
    localJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
  }

  private def doMain(_interp: ReplExec): Unit = {
    interp = _interp
    val jars = getLocalUserJarsForShell(conf)
      // Remove file:///, file:// or file:/ scheme if exists for each jar
      .map { x => if (x.startsWith("file:")) new File(new URI(x)).getPath else x }
      .mkString(File.pathSeparator)
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", jars
    )

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

      sparkSession
    } catch {
      case e: Exception if isShellSession =>
        logError("Failed to initialize Spark session.", e)
        sys.exit(1)
    }
  }

}
