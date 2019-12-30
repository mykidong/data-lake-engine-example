package mykidong.interpreter

import org.apache.spark.repl.InterpreterHelper.callMethod
import org.apache.spark.repl.SparkILoop

import scala.tools.nsc.interpreter.{IMain, NamedParam, Results, StdReplTags, isReplPower, replProps}

object InterpreterUtils {


  /**
   * zeppeline 에서 copy 함.
   *
   * @param obj
   * @param name
   * @return
   */
  def getField(obj: Object, name: String): Object = {
    val field = obj.getClass.getField(name)
    field.setAccessible(true)
    field.get(obj)
  }

  /**
   * zeppeline 에서 copy 함.
   *
   * @param interpreter
   */
  def loopPostInit(interpreter: SparkILoop): Unit = {
    import StdReplTags._
    import scala.reflect.{classTag, io}

    val sparkILoop = interpreter
    val intp = sparkILoop.intp
    val power = sparkILoop.power
    val in = sparkILoop.in

    def loopPostInit() {
      // Bind intp somewhere out of the regular namespace where
      // we can get at it in generated code.
      intp.quietBind(NamedParam[IMain]("$intp", intp)(tagOfIMain, classTag[IMain]))
      // Auto-run code via some setting.
      (replProps.replAutorunCode.option
        flatMap (f => io.File(f).safeSlurp())
        foreach (intp quietRun _)
        )
      // classloader and power mode setup
      intp.setContextClassLoader()
      if (isReplPower) {
        replProps.power setValue true
        unleashAndSetPhase()
        asyncMessage(power.banner)
      }
      // SI-7418 Now, and only now, can we enable TAB completion.
      in.postInit()
    }

    def unleashAndSetPhase() = if (isReplPower) {
      power.unleash()
      intp beSilentDuring phaseCommand("typer") // Set the phase to "typer"
    }

    def phaseCommand(name: String): Results.Result = {
      callMethod(
        sparkILoop,
        "scala$tools$nsc$interpreter$ILoop$$phaseCommand",
        Array(classOf[String]),
        Array(name)).asInstanceOf[Results.Result]
    }

    def asyncMessage(msg: String): Unit = {
      callMethod(
        sparkILoop, "asyncMessage", Array(classOf[String]), Array(msg))
    }

    loopPostInit()
  }

}
