package mykidong.interpreter

import java.io.{BufferedReader, File}
import java.nio.file.{Files, Paths}
import java.util.{Properties, UUID}

import net.liftweb.json.JObject
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{JPrintWriter, SimpleReader}

object SparkInterpreterMain extends Logging {

  initializeLogIfNecessary(true)

  var conf: SparkConf = _

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  // this is a public var because tests reset it.
  var interp: SparkILoop = _

  // TODO: Multiple User Session 에서 Concurrency Issue 는 없을까???
  /**
   * interpreter 실행후 result dataframe 을 얻기 위한 Instance.
   */
  var getBack = GetBack

  def doRun(sparkConf: SparkConf): Unit = {
      doRun(sparkConf, null)
  }


  /**
   * Spark Interpreter 생성.
   *
   * @param sparkConf Spark Configuration.
   * @param hadoopPropsArray Hadoop Configuration Properties Array.
   */
  def doRun(sparkConf: SparkConf, hadoopPropsArray: Array[Properties]): Unit = {

    this.conf = sparkConf

    // ================================= zeppeline 에서 copy 함. ============================

    /* Required for scoped mode.
     * In scoped mode multiple scala compiler (repl) generates class in the same directory.
     * Class names is not randomly generated and look like '$line12.$read$$iw$$iw'
     * Therefore it's possible to generated class conflict(overwrite) with other repl generated
     * class.
     *
     * To prevent generated class name conflict,
     * change prefix of generated class name from each scala compiler (repl) instance.
     *
     * In Spark 2.x, REPL generated wrapper class name should compatible with the pattern
     * ^(\$line(?:\d+)\.\$read)(?:\$\$iw)+$
     *
     * As hashCode() can return a negative integer value and the minus character '-' is invalid
     * in a package name we change it to a numeric value '0' which still conforms to the regexp.
     *
     */
    System.setProperty("scala.repl.name.line", ("$line" + this.hashCode).replace('-', '0'))

    val rootDir = conf.getOption("spark.repl.classdir").getOrElse(System.getProperty("java.io.tmpdir"))
    val outputDir = if(conf.getOption("spark.repl.class.outputDir").isEmpty) {
      Files.createTempDirectory(Paths.get(rootDir), "spark-" + UUID.randomUUID().toString).toFile
    } else {
      new File(conf.get("spark.repl.class.outputDir"))
    }
    //  "spark.repl.class.uri":"spark://<repl-driver-host>:<repl-driver-port>/classes" 와 같은 설정을 가진
    //  repl class fetch server 가 실행되기 위해 반드시 설정해야 함.
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)
    outputDir.deleteOnExit()

    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
    settings.usejavacp.value = true

    interp = new SparkILoop()
    interp.settings = settings
    interp.createInterpreter()

    // create spark session and spark context.
    spark2CreateContext()

    // local 실행시 hadoop configuratoin 을 설정할때.
    if(hadoopPropsArray != null) {
      val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
      import scala.collection.JavaConversions._
      for(props <- hadoopPropsArray) {
        for (key <- props.stringPropertyNames) {
          val value = props.getProperty(key)
          hadoopConfiguration.set(key, value)
        }
      }
    }

    // print pretty spark configurations.
    val json: JObject = "spark confs" -> sparkSession.sparkContext.getConf.getAll.toList
    println("spark configuration: " + prettyRender(json))

    val in0 = InterpreterUtils.getField(interp, "scala$tools$nsc$interpreter$ILoop$$in0").asInstanceOf[Option[BufferedReader]]
    val reader = in0.fold(interp.chooseReader(settings))(r => SimpleReader(r, new JPrintWriter(Console.out, true), interactive = true))

    interp.in = reader
    interp.initializeSynchronous()
    InterpreterUtils.loopPostInit(interp)
  }


  /**
   * zeppeline 에서 copy 함.
   */
  private def spark2CreateContext(): Unit = {
    val sparkClz = Class.forName("org.apache.spark.sql.SparkSession$")
    val sparkObj = sparkClz.getField("MODULE$").get(null)

    val builderMethod = sparkClz.getMethod("builder")
    val builder = builderMethod.invoke(sparkObj)
    builder.getClass.getMethod("config", classOf[SparkConf]).invoke(builder, conf)

//    if (conf.get("spark.sql.catalogImplementation", "in-memory").toLowerCase == "hive"
//      || conf.get("spark.useHiveContext", "false").toLowerCase == "true") {
//      val hiveSiteExisted: Boolean =
//        Thread.currentThread().getContextClassLoader.getResource("hive-site.xml") != null
//      val hiveClassesPresent =
//        sparkClz.getMethod("hiveClassesArePresent").invoke(sparkObj).asInstanceOf[Boolean]
//      if (hiveSiteExisted && hiveClassesPresent) {
//        builder.getClass.getMethod("enableHiveSupport").invoke(builder)
//        sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
//      } else {
//        sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
//      }
//    } else {
//      sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
//    }

    // 무조건 Hive Support!!!
    builder.getClass.getMethod("enableHiveSupport").invoke(builder)
    sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]

    sparkContext = sparkSession.getClass.getMethod("sparkContext").invoke(sparkSession)
      .asInstanceOf[SparkContext]

    interp.bind("spark", sparkSession.getClass.getCanonicalName, sparkSession, List("""@transient"""))
    interp.bind("sc", "org.apache.spark.SparkContext", sparkContext, List("""@transient"""))

    // interpreter 실행후 result dataframe 을 얻기 위한 Instance.
    interp.bind("getBack", getBack.getClass.getCanonicalName, getBack, List("""@transient"""))

    interp.interpret("import org.apache.spark.SparkContext._")
    interp.interpret("import spark.implicits._")
    interp.interpret("import spark.sql")
    interp.interpret("import org.apache.spark.sql.functions._")
    // print empty string otherwise the last statement's output of this method
    // (aka. import org.apache.spark.sql.functions._) will mix with the output of user code
    interp.interpret("print(\"\")")
  }

}
