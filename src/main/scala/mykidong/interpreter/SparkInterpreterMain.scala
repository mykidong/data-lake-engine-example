package mykidong.interpreter

import java.io.BufferedReader
import java.nio.file.{Files, Paths}

import net.liftweb.json.JObject
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.repl.{InterpreterHelper, SparkILoop}
import org.apache.spark.sql.SparkSession

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{JPrintWriter, SimpleReader}

object SparkInterpreterMain extends Logging {

  initializeLogIfNecessary(true)

  var conf: SparkConf = _

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  // this is a public var because tests reset it.
  var interp: SparkILoop = _

  var sparkHttpServer: Object = _


  def doRun(sparkConf: SparkConf): Unit = {

    this.conf = sparkConf

    // ------------- zeppellin spark interpreter 에서 가져옴...

    System.setProperty("scala.repl.name.line", ("$line" + this.hashCode).replace('-', '0'))

    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)


    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
    settings.usejavacp.value = true

    interp = new SparkILoop()
    interp.settings = settings
    interp.createInterpreter()
    spark2CreateContext()

    // print pretty spark configurations.
    val json: JObject = "spark confs" -> conf.getAll.toList
    println("spark configuration: " + prettyRender(json))

    val in0 = InterpreterHelper.getField(interp, "scala$tools$nsc$interpreter$ILoop$$in0").asInstanceOf[Option[BufferedReader]]
    val reader = in0.fold(interp.chooseReader(settings))(r => SimpleReader(r, new JPrintWriter(Console.out, true), interactive = true))

    interp.in = reader
    interp.initializeSynchronous()
    InterpreterHelper.loopPostInit(interp)
  }


  def spark2CreateContext(): Unit = {
    val sparkClz = Class.forName("org.apache.spark.sql.SparkSession$")
    val sparkObj = sparkClz.getField("MODULE$").get(null)

    val builderMethod = sparkClz.getMethod("builder")
    val builder = builderMethod.invoke(sparkObj)
    builder.getClass.getMethod("config", classOf[SparkConf]).invoke(builder, conf)

    if (conf.get("spark.sql.catalogImplementation", "in-memory").toLowerCase == "hive"
      || conf.get("spark.useHiveContext", "false").toLowerCase == "true") {
      val hiveSiteExisted: Boolean =
        Thread.currentThread().getContextClassLoader.getResource("hive-site.xml") != null
      val hiveClassesPresent =
        sparkClz.getMethod("hiveClassesArePresent").invoke(sparkObj).asInstanceOf[Boolean]
      if (hiveSiteExisted && hiveClassesPresent) {
        builder.getClass.getMethod("enableHiveSupport").invoke(builder)
        sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
      } else {
        sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
      }
    } else {
      sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
    }

    sparkContext = sparkSession.getClass.getMethod("sparkContext").invoke(sparkSession)
      .asInstanceOf[SparkContext]

    interp.bind("spark", sparkSession.getClass.getCanonicalName, sparkSession, List("""@transient"""))
    interp.bind("sc", "org.apache.spark.SparkContext", sparkContext, List("""@transient"""))

    interp.interpret("import org.apache.spark.SparkContext._")
    interp.interpret("import spark.implicits._")
    interp.interpret("import spark.sql")
    interp.interpret("import org.apache.spark.sql.functions._")
    // print empty string otherwise the last statement's output of this method
    // (aka. import org.apache.spark.sql.functions._) will mix with the output of user code
    interp.interpret("print(\"\")")
  }

}
