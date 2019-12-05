package mykidong.http

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import io.shaka.http.HttpServer
import io.shaka.http.Request.POST
import io.shaka.http.Response.respond
import io.shaka.http.Status.NOT_FOUND
import org.apache.spark.SparkConf
import org.apache.spark.repl.ReplMain
import org.slf4j.LoggerFactory

object SimpleHTTPServer {

  private val log = LoggerFactory.getLogger(getClass.getName)

  private var conf: SparkConf = _

  def run(conf: SparkConf, port: Int): Unit = {
    this.conf = conf

    // start http server.
    val httpServer = HttpServer(port).start()


    // TODO: 넘어온 spark conf 를 Repl 에 넘겨야 되는데 어떻게 할까.
    // TODO: // set fair scheduler pool.
    //       jsc.setLocalProperty("spark.scheduler.pool", "pool-" + Thread.currentThread.getId)
    //       ............
    //       // unset fair scheduler pool.
    //       jsc.setLocalProperty("spark.scheduler.pool", null)

    ReplMain.main(Array(""))
    val interpreter = ReplMain.interp

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
        try {

          interpreter.command(codes)

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
