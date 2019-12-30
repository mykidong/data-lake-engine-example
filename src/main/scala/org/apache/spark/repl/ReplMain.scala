package org.apache.spark.repl

import java.io.{BufferedReader, File}
import java.net.URI
import java.util.Locale

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.util.Utils

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.{JPrintWriter, SimpleReader}

object ReplMain extends Logging {

  initializeLogIfNecessary(true)
  Signaling.cancelOnInterrupt()

  var conf: SparkConf = _
  var rootDir: String = _
  var outputDir: File = _

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  // this is a public var because tests reset it.
  var interp: Interpreter = _

  private var hasErrors = false
  private var isShellSession = false

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    // scalastyle:off println
    Console.err.println(msg)
    // scalastyle:on println
  }

  /**
   * REPL 을 실행하지 않고 Interpreter 만 사용할 경우.
   *
   */
  def doRun(sparkConf: SparkConf): Unit = {

    this.conf = sparkConf
    println(s"spark configuration: ${this.conf.getAll.toList.toString()}")

    rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))

    if(conf.getOption("spark.repl.class.outputDir").isEmpty) {
      outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")
    }
    // repl 이 local 에서 실행될경우.
    else {
      outputDir = new File(conf.get("spark.repl.class.outputDir"))
    }


    isShellSession = true

    interp = new Interpreter()
    val jars = Utils.getLocalUserJarsForShell(conf)
      // Remove file:///, file:// or file:/ scheme if exists for each jar
      .map { x => if (x.startsWith("file:")) new File(new URI(x)).getPath else x }
      .mkString(File.pathSeparator)
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", jars
    )

    println(s"interpArguments: ${interpArguments.toString()}")

    val settings = new GenericRunnerSettings(scalaOptionError)
    settings.processArguments(interpArguments, true)

    // ------------- zeppellin spark interpreter 에서 가져옴...
    settings.usejavacp.value = true
    interp.settings = settings
    interp.createInterpreter()
    interp.initializeSpark()



    val in0 = InterpreterHelper.getField(interp, "scala$tools$nsc$interpreter$ILoop$$in0").asInstanceOf[Option[BufferedReader]]
    val reader = in0.fold(interp.chooseReader(settings))(r => SimpleReader(r, new JPrintWriter(Console.out, true), interactive = true))

    interp.in = reader
    interp.initializeSynchronous()
    InterpreterHelper.loopPostInit(interp)
  }

  /**
   * REPL 을 실행할 경우
   *
   * NOTE: 개발 현재 REPL 은 사용하지 않음.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    isShellSession = true
    doMain(args, new Interpreter())
  }

  /**
   * REPL 을 실행할 경우.
   *
   * NOTE: 개발 현재 REPL 은 사용하지 않음.
   *
   * @param args
   * @param _interp
   */
  private[repl] def doMain(args: Array[String], _interp: Interpreter): Unit = {
    interp = _interp
    val jars = Utils.getLocalUserJarsForShell(conf)
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
        if (SparkSession.hiveClassesArePresent) {
          // In the case that the property is not set at all, builder's config
          // does not have this value set to 'hive' yet. The original default
          // behavior is that when there are hive classes, we use hive catalog.
          sparkSession = builder.enableHiveSupport().getOrCreate()
          logInfo("Created Spark session with Hive support")
        } else {
          // Need to change it back to 'in-memory' if no hive classes are found
          // in the case that the property is set to hive in spark-defaults.conf
          builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
          sparkSession = builder.getOrCreate()
          logInfo("Created Spark session")
        }
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
