package mykidong.http

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import io.shaka.http.HttpServer
import io.shaka.http.Request.POST
import io.shaka.http.Response.respond
import io.shaka.http.Status.NOT_FOUND
import mykidong.reflect.DynamicScalaSparkJobRunner
import org.apache.spark.SparkContext
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.{GenericRunnerSettings, Settings}
import scala.tools.reflect.ToolBox

object SimpleHTTPServer {

  private val log = LoggerFactory.getLogger(getClass.getName)

  def run(spark: SparkSession, sc: SparkContext, port: Int): Unit = {

    val httpServer = HttpServer(port).start()

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
          val rootDir = spark.conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
          val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
          outputDir.deleteOnExit()
          spark.conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

          val settings = new Settings()
          settings.processArguments(List("-Yrepl-class-based",
            "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
          settings.usejavacp.value = true

          val out = System.out
          val flusher = new java.io.PrintWriter(out)
          val interpreter = new IMain(settings, flusher)

          val sc = spark.sparkContext;
          interpreter.bind("sc", sc);
          val resultFlag = interpreter.interpret(codes)

          var res = Array[AnyRef](null)
          interpreter.bind("result", "Array[AnyRef]", res)
          interpreter.interpret("result(0) = classOf[DynamicScalaSparkJobRunner]")
          val dynamicSparkRunner = res(0).asInstanceOf[Class[_]].getConstructors()(0).newInstance().asInstanceOf[DynamicScalaSparkJobRunner]
          retValue = dynamicSparkRunner.run(spark)
          log.info("retValue: [" + retValue + "]")

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
          sc.setLocalProperty("spark.scheduler.pool", null)
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
}
