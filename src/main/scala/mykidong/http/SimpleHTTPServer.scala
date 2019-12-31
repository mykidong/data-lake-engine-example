package mykidong.http

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.Properties

import io.shaka.http.HttpServer
import io.shaka.http.Request.POST
import io.shaka.http.Response.respond
import io.shaka.http.Status.NOT_FOUND
import mykidong.interpreter.SparkInterpreterMain
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

object SimpleHTTPServer {

  private val log = LoggerFactory.getLogger(getClass.getName)

  private var conf: SparkConf = _

  def run(conf: SparkConf, port: Int): Unit = {
      run(conf, null, port)
  }

  def run(conf: SparkConf, propsArray: Array[Properties], port: Int): Unit = {
    this.conf = conf

    // start http server.
    val httpServer = HttpServer(port).start()

    // run interpreter main with spark conf.
    SparkInterpreterMain.doRun(conf, propsArray)
    val interpreter = SparkInterpreterMain.interp

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
        println("fullClassName: [" + fullClassName + "]");

        // codes param.
        val codes = paramMap.get("codes").get
        println("codes: [" + codes + "]");

        var retValue = ""

        val startTime = System.currentTimeMillis

        // set scheduler pool for the current thread.
        SparkInterpreterMain.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
        println("before running spark codes, scheduler pool set to [" + SparkInterpreterMain.sparkContext.getLocalProperty("spark.scheduler.pool") + "] for the current thread [" + Thread.currentThread().getId + "]");

        try {
          // print current scheduler pool in repl.
          interpreter.command("println(\"REPL: before running spark codes, scheduler pool set to [\" + sc.getLocalProperty(\"spark.scheduler.pool\") + \"] for the current thread [\" + Thread.currentThread().getId + \"]\")")

          // interpret spark codes.
          interpreter.command(codes)

          val mostRecentVar = interpreter.mostRecentVar;
          println("mostRecentVar: [" + mostRecentVar + "]")

          val typeOfMostRecentVar = interpreter.typeOfTerm(interpreter.mostRecentVar)
          println("typeOfMostRecentVar: [" + typeOfMostRecentVar + "]")

          // TODO: 왜 valueOfTerm 값이 아무것도 없을까...
          val getBack = interpreter.valueOfTerm("getBack").getOrElse(null)
//          val mostRecentVarValue = interpreter.valueOfTerm(interpreter.mostRecentVar).getOrElse(null)
          println("getBack: [" + getBack + "]")
        } catch {
          case e: Exception => {
            e.printStackTrace()
          }
        }

        // unset scheduler pool for the current thread.
        SparkInterpreterMain.sparkContext.setLocalProperty("spark.scheduler.pool", null)
        println("after spark codes run, scheduler pool set to [" + SparkInterpreterMain.sparkContext.getLocalProperty("spark.scheduler.pool") + "] for the current thread [" + Thread.currentThread().getId + "]");

        // print current scheduler pool in repl.
        interpreter.command("println(\"REPL: after spark codes run, scheduler pool set to [\" + sc.getLocalProperty(\"spark.scheduler.pool\") + \"] for the current thread [\" + Thread.currentThread().getId + \"]\")")

        println("elapsed time: [" + (System.currentTimeMillis - startTime).toDouble / 1000.toDouble + "]s")
        println("requested spark job is done...")

        respond(retValue)
      }
      case _ => respond("doh!").status(NOT_FOUND)
    }
  }
}
