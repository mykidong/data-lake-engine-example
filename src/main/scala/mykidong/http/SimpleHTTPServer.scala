package mykidong.http

import io.shaka.http.HttpServer
import io.shaka.http.Request.POST
import io.shaka.http.Response.respond
import io.shaka.http.Response.seeOther
import io.shaka.http.Status.NOT_FOUND


object SimpleHTTPServer {

  def main(args: Array[String]): Unit = {
    val port = 8125
    val httpServer = HttpServer(port).start()

    httpServer.handler{
      case request@POST("/run-codes") => {
        val value = request.entityAsString
        println("value: " + value)
        respond("OK")
      }
      case _ => respond("doh!").status(NOT_FOUND)
    }
  }

}
