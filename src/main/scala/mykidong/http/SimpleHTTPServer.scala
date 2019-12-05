package mykidong.http

import java.io.{BufferedReader, OutputStreamWriter, StringReader, StringWriter}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import io.shaka.http.HttpServer
import io.shaka.http.Request.POST
import io.shaka.http.Response.respond
import io.shaka.http.Status.NOT_FOUND
import mykidong.repl.{ReplExec, ReplMain}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.{JPrintWriter, NamedParam, SimpleReader}
import scala.tools.nsc.util.stringFromStream

object SimpleHTTPServer {

  private val log = LoggerFactory.getLogger(getClass.getName)

  private var conf: SparkConf = _

  def run(conf: SparkConf, port: Int): Unit = {
    this.conf = conf

    // start http server.
    val httpServer = HttpServer(port).start()

    // ========================= init. repl.
    System.setProperty("scala.usejavacp", "true")
//
//    org.apache.spark.repl.Main.main(Array(""))
//    val repl = org.apache.spark.repl.Main.interp

    ReplMain.main(Array(""))
    val repl = ReplMain.interp


//    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
//    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
//    outputDir.deleteOnExit()
//
//    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)
//    log.info("spark.repl.class.outputDir: [" + outputDir.getAbsolutePath + "]");
//
//    val settings = new GenericRunnerSettings(println _)
//    settings.processArguments(List("-Yrepl-class-based",
//      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
//    settings.usejavacp.value = true
//    if (settings.classpath.isDefault) {
//      settings.classpath.value = sys.props("java.class.path")
//    }
//
//    val replOut = new JPrintWriter(Console.out, true)
//
//    val repl = new ReplExec(None, replOut)
//    repl.settings = settings
//    repl.createInterpreter()
//    repl.initializeSpark()
//
//    val in0 = getField(repl, "scala$tools$nsc$interpreter$ILoop$$in0").asInstanceOf[Option[BufferedReader]]
//    val reader = in0.fold(repl.chooseReader(settings))(r => SimpleReader(r, replOut, interactive = true))
//
//    repl.in = reader
//    repl.initializeSynchronous()
//    repl.loopPostInit()

    httpServer.handler{
      case request@POST("/run-codes") => {

        val start: Long = System.currentTimeMillis

        val body: String = request.entityAsString

        var paramMap: Map[String, String] = Map.empty

        val paramArray: Array[String] = body.split("&")

        val urlDecode = (str: String) => {
          URLDecoder.decode(str, StandardCharsets.UTF_8.toString())
        }

        paramArray.foreach(paramString => {
          val kvArray: Array[String] = paramString.split("=")
          paramMap = paramMap + (kvArray(0) -> urlDecode(kvArray(1)))
        })

        // full class name param.
        val fullClassName = paramMap.get("fullClassName").get
        log.info("fullClassName: [" + fullClassName + "]");

        // codes param.
        val codes = paramMap.get("codes").get
        log.info("codes: [" + codes + "]");

        var retValue = ""
        try {
          val lines = codes.split("\n")
          lines.foreach(line => {
            log.info("ready to run command: [" + line + "]")

            val result = repl.command(line)
            log.info("result: [" + result.toString + "]")
          })
//

//
//          var repl = new ReplExec(None, new JPrintWriter(Console.out, true))
//          repl.settings = settings
//          repl.createInterpreter()
//          repl.initializeSpark()
//
//          val lines = codes.split("\n")
//          lines.foreach(line => {
//            log.info("ready to run command: [" + line + "]")
//            repl.command(line)
//          })
//
//          repl.process(settings)
//





//          val repl = new ReplExec(None, new JPrintWriter(Console.out, true))
//          if (settings.classpath.isDefault) {
//            settings.classpath.value = sys.props("java.class.path")
//          }
//          repl.process(settings)
//          log.info("proocess settings done...")
//
//          repl.intp.quietRun(codes)
//          log.info("quietRun done...")


//          val out = System.out
//          val flusher = new java.io.PrintWriter(out)
//          val interpreter = new IMain(settings, flusher)
//
//          val sc = spark.sparkContext;
//          interpreter.bind("sc", sc);
//          val res = interpreter.interpret(codes)
//
//          val dynamicSparkRunner = res.asInstanceOf[Class[_]].getConstructors()(0).newInstance().asInstanceOf[DynamicScalaSparkJobRunner]
//          retValue = dynamicSparkRunner.run(spark)
//          log.info("retValue: [" + retValue + "]")

//          val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
//          val ast = tb.parse(codes);
//          val clazz = tb.compile(ast)().asInstanceOf[Class[_]]
//          val ctor = clazz.getDeclaredConstructors()(0)
//
//          // dynamic spark runner.
//          val dynamicSparkRunner = ctor.newInstance().asInstanceOf[DynamicScalaSparkJobRunner]
//
//          // set fair scheduler pool.
//          sc.setLocalProperty("spark.scheduler.pool", "pool-" + Thread.currentThread.getId)
//
//          // execute run().
//          retValue = dynamicSparkRunner.run(spark)
//          log.info("retValue: [" + retValue + "]")

          log.info("elapsed time: [" + (System.currentTimeMillis - start).toDouble / 1000.toDouble + "]s")
          log.info("requested spark job is done...")

          // unset fair scheduler pool.
//          sc.setLocalProperty("spark.scheduler.pool", null)
        } catch {
          case e: Exception => {
            e.printStackTrace()
          }
        }

        respond(retValue)
      }
      case _ => respond("doh!").status(NOT_FOUND)
    }
  }

  def getField(obj: Object, name: String): Object = {
    val field = obj.getClass.getField(name)
    field.setAccessible(true)
    field.get(obj)
  }
}
