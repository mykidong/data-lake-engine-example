package mykidong.utils

import mykidong.reflect.DynamicScalaSparkJobRunner
import mykidong.util.StringUtils
import org.scalatest.FunSuite
import org.scalatest._

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox


class DynamicCompileSpec extends FunSuite{

  test("runDynamicCompilation") {
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    println("tb: " + tb.toString)

    // read java codes.
    val codes = StringUtils.fileToString("/templates/save-as-table-request.scala")
    println("codes: \n" + codes)

    // TODO: parsing problem...
    val ast = tb.parse(codes);
    println("ast: " + ast.toString)

    val clazz = tb.compile(ast)().asInstanceOf[Class[_]]
    println("clazz: " + clazz.toString)

    val ctor = clazz.getDeclaredConstructors()(0)
    println("ctor: " + ctor.toString)

    val dynamicSparkRunner = ctor.newInstance().asInstanceOf[DynamicScalaSparkJobRunner]
    println("dynamicSparkRunner: " + dynamicSparkRunner.run(null))
  }
}
