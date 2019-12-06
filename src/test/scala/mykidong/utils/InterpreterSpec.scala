package mykidong.utils

import org.scalatest.FunSuite

class InterpreterSpec extends FunSuite{

  test("value of term") {

    object Interpreter {
      import scala.tools.nsc._
      import scala.tools.nsc.interpreter._

      class Dummy

      val settings = new Settings
      settings.usejavacp.value = true
      settings.embeddedDefaults[Dummy]  // to make imain useable with sbt.

      val imain = new IMain(settings)

      def run(code: String) = {
        this.imain.beQuietDuring{
          this.imain.interpret(code)
        }
        val ret = this.imain.valueOfTerm(this.imain.mostRecentVar)
        this.imain.reset()
        ret
      }
    }


    println(Interpreter.run("val x = 1; val y = 5; val z = x + y;"))

  }

}
