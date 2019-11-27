package mykidong.http

import org.scalatest.FunSuite

class SimpleHTTPServerSpec extends FunSuite{

  test("runSimpleHttpServer") {
    SimpleHTTPServer.run(null, null, 8125)

    Thread.sleep(Long.MaxValue)
  }

}
