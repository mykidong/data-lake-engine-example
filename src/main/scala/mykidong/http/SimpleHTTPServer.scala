package mykidong.http

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import io.shaka.http.HttpServer
import io.shaka.http.Request.POST
import io.shaka.http.Response.respond
import io.shaka.http.Status.NOT_FOUND
import mykidong.reflect.DynamicScalaSparkJobRunner
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe
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
          val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
          log.info("tb: [" + tb.toString + "]");

          val ast = tb.parse(codes);
          log.info("ast: [" + ast.toString() + "]");

          val clazz = tb.compile(ast)().asInstanceOf[Class[_]]
          log.info("clazz: [" + clazz.getName + "]");

          val ctor = clazz.getDeclaredConstructors()(0)
          log.info("ctor: [" + ctor.getName + "]");

          // dynamic spark runner.
          val dynamicSparkRunner = ctor.newInstance().asInstanceOf[DynamicScalaSparkJobRunner]
          log.info("dynamicSparkRunner: [" + dynamicSparkRunner.getClass.getName + "]");

          // set fair scheduler pool.
          sc.setLocalProperty("spark.scheduler.pool", "pool-" + Thread.currentThread.getId)

          // execute run().
          retValue = dynamicSparkRunner.run(spark)
          println("retValue: [" + retValue + "]")

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
