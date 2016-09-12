package ru.hokan

import scala.collection.mutable.ListBuffer
import scala.io.Source

object MLResultParser {

  val FLOAT_REGEXP = "\\d\\.\\d+"
  val REGEXP_PATTERN = "\\[(" + FLOAT_REGEXP + "),\\[(" + FLOAT_REGEXP + "),(" + FLOAT_REGEXP + ").*,(" + FLOAT_REGEXP + ")\\]"
  val REGEXP = REGEXP_PATTERN.r

  def main(args: Array[String]): Unit = {
    val tuples = ListBuffer[Tuple]()
    val filename = "src/main/result/part-00000"
    for (line <- Source.fromFile(filename).getLines()) {
      val REGEXP(expected, probabilityHigh, probabilityLow, actual) = line
      tuples += Tuple(expected.toFloat, probabilityHigh.toFloat, probabilityLow.toFloat, actual.toFloat)
    }

    tuples.foreach(println)
  }

  private case class Tuple(expected : Float, probabilityHigh : Float, probabilityLow : Float, actual : Float)
}
