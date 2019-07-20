package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new StackOverflow {
    override val langs =
      List("JavaScript",
           "Java",
           "PHP",
           "Python",
           "C#",
           "C++",
           "Ruby",
           "CSS",
           "Objective-C",
           "Perl",
           "Scala",
           "Haskell",
           "MATLAB",
           "Clojure",
           "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("medianCalculator works on even-sized lists") {
    val evenSet = Seq(1, 5, 14, 16, 17, 20)
    assert(StackOverflow.medianCalculator(evenSet) == 15, "medianCalculator doesn't work on even-sized lists")
  }

  test("medianCalculator works on odd-sized lists") {
    val oddSet = Seq(3, 5, 10, 11, 19)
    assert(StackOverflow.medianCalculator(oddSet) == 10, "medianCalculator doesn't work on even-sized lists")
  }
}
