package mykidong.utils

import java.lang.reflect.Field

import org.apache.spark.internal.config._
import org.apache.spark.{SparkConf, SparkEnv}
import org.scalatest.FunSuite

class ReplClassLoaderSpec extends FunSuite {

  test("load remote classes via repl classes uri") {

    val currentClassLoader: ClassLoader = Thread.currentThread.getContextClassLoader

    val classLoader: ClassLoader = addReplClassLoaderIfNeeded(currentClassLoader.asInstanceOf[ClassLoader]).asInstanceOf[ClassLoader]

    def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
      case null => Array()
      case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
      case _ => urlses(cl.getParent)
    }

    val  urls = urlses(classLoader)
    println(s"classpath: ${urls.mkString("\n")}")

  }

  private def addReplClassLoaderIfNeeded(parent: ClassLoader) = {
    val conf = new SparkConf()
    conf.set("spark.repl.class.uri", "spark://mc-d02.opasnet.io:44571/classes")

    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      println("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: Boolean = false
        val klass = classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf,
                                SparkEnv.get,
                                classUri,
                                parent,
                                _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          println("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Spark.
   *
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
   */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  private def classForName[C](
                       className: String,
                       initialize: Boolean = true,
                       noSparkClassLoader: Boolean = false): Class[C] = {
    if (!noSparkClassLoader) {
      Class.forName(className, initialize, getContextOrSparkClassLoader).asInstanceOf[Class[C]]
    } else {
      Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
        asInstanceOf[Class[C]]
    }
    // scalastyle:on classforname
  }
}
