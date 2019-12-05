package org.apache.spark.repl

import java.io.BufferedReader

import scala.tools.nsc.interpreter.{Results, StdReplTags}

// scalastyle:off println
import scala.Predef.{println => _}
// scalastyle:on println
import scala.concurrent.Future
import scala.reflect.classTag
import scala.reflect.io.File
import scala.tools.nsc.interpreter.StdReplTags.tagOfIMain
import scala.tools.nsc.interpreter.{AbstractOrMissingHandler, ILoop, IMain, JPrintWriter, NamedParam, SimpleReader, SplashLoop, SplashReader, isReplDebug, isReplPower, replProps}
import scala.tools.nsc.util.stringFromStream
import scala.tools.nsc.{GenericRunnerSettings, Properties, Settings}
import scala.util.Properties.{javaVersion, javaVmName, versionString}

/**
 *  A Spark-specific interactive shell.
 */
class ReplExec(in0: Option[BufferedReader], out: JPrintWriter)
  extends ILoop(in0, out) {
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

  val initializationCommands: Seq[String] = Seq(
    """
    @transient val spark = if (org.apache.spark.repl.ReplMain.sparkSession != null) {
        org.apache.spark.repl.ReplMain.sparkSession
      } else {
        org.apache.spark.repl.ReplMain.createSparkSession()
      }
    @transient val sc = {
      val _sc = spark.sparkContext
      if (_sc.getConf.getBoolean("spark.ui.reverseProxy", false)) {
        val proxyUrl = _sc.getConf.get("spark.ui.reverseProxyUrl", null)
        if (proxyUrl != null) {
          println(
            s"Spark Context Web UI is available at ${proxyUrl}/proxy/${_sc.applicationId}")
        } else {
          println(s"Spark Context Web UI is available at Spark Master Public URL")
        }
      } else {
        _sc.uiWebUrl.foreach {
          webUrl => println(s"Spark context Web UI available at ${webUrl}")
        }
      }
      println("Spark context available as 'sc' " +
        s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
      println("Spark session available as 'spark'.")
      _sc
    }
    """,
    "import org.apache.spark.SparkContext._",
    "import spark.implicits._",
    "import spark.sql",
    "import org.apache.spark.sql.functions._"
  )

  def initializeSpark(): Unit = {
    if (!intp.reporter.hasErrors) {
      // `savingReplayStack` removes the commands from session history.
      savingReplayStack {
        initializationCommands.foreach(intp quietRun _)
      }
    } else {
      throw new RuntimeException(s"Scala $versionString interpreter encountered " +
        "errors during initialization")
    }
  }

  /** Print a welcome message */
  override def printWelcome(): Unit = {
    import org.apache.spark.SPARK_VERSION
    echo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  /** Available commands */
  override def commands: List[LoopCommand] = standardCommands

  override def resetCommand(line: String): Unit = {
    super.resetCommand(line)
    initializeSpark()
    echo("Note that after :reset, state of SparkSession and SparkContext is unchanged.")
  }

  override def replay(): Unit = {
    initializeSpark()
    super.replay()
  }

  /**
   * The following code is mostly a copy of `process` implementation in `ILoop.scala` in Scala
   *
   * In newer version of Scala, `printWelcome` is the first thing to be called. As a result,
   * SparkUI URL information would be always shown after the welcome message.
   *
   * However, this is inconsistent compared with the existing version of Spark which will always
   * show SparkUI URL first.
   *
   * The only way we can make it consistent will be duplicating the Scala code.
   *
   * We should remove this duplication once Scala provides a way to load our custom initialization
   * code, and also customize the ordering of printing welcome message.
   */
  override def process(settings: Settings): Boolean = {

    def newReader = in0.fold(chooseReader(settings))(r => SimpleReader(r, out, interactive = true))

    /** Reader to use before interpreter is online. */
    def preLoop = {
      val sr = SplashReader(newReader) { r =>
        in = r
        in.postInit()
      }
      in = sr
      SplashLoop(sr, prompt)
    }

    /* Actions to cram in parallel while collecting first user input at prompt.
     * Run with output muted both from ILoop and from the intp reporter.
     */
    def loopPostInit(): Unit = mumly {
      // Bind intp somewhere out of the regular namespace where
      // we can get at it in generated code.
      intp.quietBind(NamedParam[IMain]("$intp", intp)(tagOfIMain, classTag[IMain]))

      // Auto-run code via some setting.
      ( replProps.replAutorunCode.option
        flatMap (f => File(f).safeSlurp())
        foreach (intp quietRun _)
        )
      // power mode setup
      if (isReplPower) enablePowerMode(true)
      initializeSpark()
      loadInitFiles()
      // SI-7418 Now, and only now, can we enable TAB completion.
      in.postInit()
    }
    def loadInitFiles(): Unit = settings match {
      case settings: GenericRunnerSettings =>
        for (f <- settings.loadfiles.value) {
          loadCommand(f)
          addReplay(s":load $f")
        }
        for (f <- settings.pastefiles.value) {
          pasteCommand(f)
          addReplay(s":paste $f")
        }
      case _ =>
    }
    // wait until after startup to enable noisy settings
    def withSuppressedSettings[A](body: => A): A = {
      val ss = this.settings
      import ss._
      val noisy = List(Xprint, Ytyperdebug)
      val noisesome = noisy.exists(!_.isDefault)
      val current = (Xprint.value, Ytyperdebug.value)
      if (isReplDebug || !noisesome) body
      else {
        this.settings.Xprint.value = List.empty
        this.settings.Ytyperdebug.value = false
        try body
        finally {
          Xprint.value = current._1
          Ytyperdebug.value = current._2
          intp.global.printTypings = current._2
        }
      }
    }
    def startup(): String = withSuppressedSettings {
      // let them start typing
      val splash = preLoop

      // while we go fire up the REPL
      try {
        // don't allow ancient sbt to hijack the reader
        savingReader {
          createInterpreter()
        }
        intp.initializeSynchronous()

        val field = classOf[ILoop].getDeclaredFields.filter(_.getName.contains("globalFuture")).head
        field.setAccessible(true)
        field.set(this, Future successful true)

        if (intp.reporter.hasErrors) {
          echo("Interpreter encountered errors during initialization!")
          null
        } else {
          loopPostInit()
          printWelcome()
          splash.start()

          val line = splash.line           // what they typed in while they were waiting
          if (line == null) {              // they ^D
            try out print Properties.shellInterruptedString
            finally closeInterpreter()
          }
          line
        }
      } finally splash.stop()
    }

    this.settings = settings
    startup() match {
      case null => false
      case line =>
        try loop(line) match {
          case LineResults.EOF => out print Properties.shellInterruptedString
          case _ =>
        }
        catch AbstractOrMissingHandler()
        finally closeInterpreter()
        true
    }
  }


  /**
   * zepellin 추가 method.
   */
  def getField(obj: Object, name: String): Object = {
    val field = obj.getClass.getField(name)
    field.setAccessible(true)
    field.get(obj)
  }

  /**
   * zepellin 추가 method.
   */
  def loopPostInit(): Unit = {
    import StdReplTags._
    import scala.reflect.{classTag, io}

    val sparkILoop = this
    val intp = this.intp
    val power = this.power
    val in = this.in

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

    def callMethod(obj: Object, name: String,
                   parameterTypes: Array[Class[_]],
                   parameters: Array[Object]): Object = {
      val method = obj.getClass.getMethod(name, parameterTypes: _ *)
      method.setAccessible(true)
      method.invoke(obj, parameters: _ *)
    }

    loopPostInit()
  }
}

object ReplExec {

  /**
   * Creates an interpreter loop with default settings and feeds
   * the given code to it as input.
   */
  def run(code: String, sets: Settings = new Settings): String = {
    import java.io.{BufferedReader, OutputStreamWriter, StringReader}

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val input = new BufferedReader(new StringReader(code))
        val output = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl = new ReplExec(input, output)

        if (sets.classpath.isDefault) {
          sets.classpath.value = sys.props("java.class.path")
        }
        repl process sets
      }
    }
  }
  def run(lines: List[String]): String = run(lines.map(_ + "\n").mkString)
}
