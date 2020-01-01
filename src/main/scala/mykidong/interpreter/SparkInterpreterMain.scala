package mykidong.interpreter

import java.io.{BufferedReader, File}
import java.lang.reflect.Method
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.{Locale, Properties, UUID}

import net.liftweb.json.JObject
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.{JPrintWriter, SimpleReader}

object SparkInterpreterMain extends Logging {

  initializeLogIfNecessary(true)

  var conf: SparkConf = _
  var outputDir: File = _

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  // this is a public var because tests reset it.
  var interp: SparkILoop = _

  var replLocalJars: Seq[String] = _

  // TODO: Multiple User Session 에서 Concurrency Issue 는 없을까???
  /**
   * interpreter 실행후 result dataframe 을 얻기 위한 Instance.
   */
  var getBack = GetBack


  private var hasErrors = false
  private var isShellSession = false

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    // scalastyle:off println
    Console.err.println(msg)
    // scalastyle:on println
  }

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
    outputDir = if(conf.getOption("spark.repl.class.outputDir").isEmpty) {
      Files.createTempDirectory(Paths.get(rootDir), "spark-repl-" + UUID.randomUUID().toString).toFile
    } else {
      new File(conf.get("spark.repl.class.outputDir"))
    }
    outputDir.deleteOnExit()


    val replOutput = new JPrintWriter(Console.out, true)
    interp = new SparkILoop(None, replOutput)


    // org.apache.spark.util.Utils object 를 여기에서 접근할수 없기 때문에 reflection 을 이용하여 Method Access 함.
    import scala.reflect.runtime.universe

    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule("org.apache.spark.util.Utils")
    val obj = runtimeMirror.reflectModule(module).instance

    var sparkUtilsClzMethod: Method = null

    import scala.util.control.Breaks._
    breakable {
      for (method: Method <- obj.getClass.getDeclaredMethods) {
        // Method: Utils.getLocalUserJarsForShell()
        if (method.getName.equals("getLocalUserJarsForShell")) {
          sparkUtilsClzMethod = method
          sparkUtilsClzMethod.setAccessible(true)

          break
        }
      }
    }

    // repl local jar files.
    replLocalJars = sparkUtilsClzMethod.invoke(obj, conf).asInstanceOf[Seq[String]]

    val jars = replLocalJars
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

    settings.usejavacp.value = true
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
    val reader = in0.fold(interp.chooseReader(settings))(r => SimpleReader(r, replOutput, interactive = true))

    interp.in = reader
    interp.initializeSynchronous()
    InterpreterUtils.loopPostInit(interp)
  }

  private def spark2CreateContext(): Unit = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    conf.setIfMissing("spark.app.name", this.getClass.getName)

    //  "spark.repl.class.uri":"spark://<repl-driver-host>:<repl-driver-port>/classes" 와 같은 설정을 가진
    //  repl class fetch server 가 실행되기 위해 반드시 설정해야 함.
    //
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

    val sparkClz = Class.forName("org.apache.spark.sql.SparkSession$")
    val sparkObj = sparkClz.getField("MODULE$").get(null)

    val builderMethod = sparkClz.getMethod("builder")
    val builderBase = builderMethod.invoke(sparkObj)
    val builder = builderBase.getClass.getMethod("config", classOf[SparkConf]).invoke(builderBase, conf).asInstanceOf[SparkSession.Builder]

    if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase(Locale.ROOT) == "hive") {
      val hiveClassesPresent = sparkClz.getMethod("hiveClassesArePresent").invoke(sparkObj).asInstanceOf[Boolean]
      if (hiveClassesPresent) {
        builder.getClass.getMethod("enableHiveSupport").invoke(builder)
        sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
        println("Created Spark session (with Hive support).\n")
      } else {
        builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
        sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
        println("Created Spark session.\n")
      }
    } else {
      sparkSession = builder.getClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[SparkSession]
      println("Created Spark session.\n")
    }

    sparkContext = sparkSession.getClass.getMethod("sparkContext").invoke(sparkSession)
      .asInstanceOf[SparkContext]
    replLocalJars.foreach(file => sparkContext.addFile(file))


    interp.bind("spark", sparkSession.getClass.getCanonicalName, sparkSession, List("""@transient"""))
    interp.bind("sc", "org.apache.spark.SparkContext", sparkContext, List("""@transient"""))

    // interpreter 실행후 result dataframe 을 얻기 위한 Instance.
    interp.bind("getBack", getBack.getClass.getCanonicalName, getBack, List("""@transient"""))

    interp.interpret("import org.apache.spark.SparkContext._")
    interp.interpret("import spark.implicits._")
    interp.interpret("import spark.sql")
    interp.interpret("import org.apache.spark.sql.functions._")
  }

}
