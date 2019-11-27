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
    val codes = StringUtils.fileToString("/templates/count.scala")
    println("codes: \n" + codes)

    val ast = tb.parse(codes);
    println("ast: " + ast.toString)

    val clazz = tb.compile(ast)().asInstanceOf[Class[_]]
    println("clazz: " + clazz.getName)

    val ctor = clazz.getDeclaredConstructors()(0)
    println("ctor: " + ctor.getName)

    val dynamicSparkRunner = ctor.newInstance().asInstanceOf[DynamicScalaSparkJobRunner]
    println("dynamicSparkRunner retValue: " + dynamicSparkRunner.run(null))
  }
}
