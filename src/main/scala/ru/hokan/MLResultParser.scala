package ru.hokan

import scala.collection.mutable.ListBuffer
import scala.io.Source

object MLResultParser {

  val FLOAT_REGEXP = "\\d\\.\\d+"
  val REGEXP_PATTERN = "\\[(" + FLOAT_REGEXP + "),\\[(" + FLOAT_REGEXP + "),(" + FLOAT_REGEXP + ").*,(" + FLOAT_REGEXP + ")\\]"
  val REGEXP = REGEXP_PATTERN.r
  val INTERVAL_RANGE_IN_PERCENT = 0.05

  def main(args: Array[String]): Unit = {
    val tuples = ListBuffer[Tuple]()
    val filename = "src/main/result/part-00000"
    for (line <- Source.fromFile(filename).getLines()) {
      val REGEXP(expected, probabilityHigh, probabilityLow, actual) = line
      tuples += Tuple(expected.toFloat, probabilityHigh.toFloat, probabilityLow.toFloat, actual.toFloat)
    }

    val rates = ListBuffer[Rate]()
    val BLOCK_SIZE: Double = tuples.length * INTERVAL_RANGE_IN_PERCENT
    var j = 0
    val tempValues: Values = Values(0, 0, 0, 0)
    tuples.foreach(tuple => {

      if (tuple.actual == 0 && tuple.expected == 0) {
        tempValues.TN += 1
      } else if (tuple.actual == 0 && tuple.expected == 1) {
        tempValues.FN += 1
      } else if (tuple.actual == 1 && tuple.expected == 0) {
        tempValues.FP += 1
      } else if (tuple.actual == 1 && tuple.expected == 1) {
        tempValues.TP += 1
      }

      if (j > BLOCK_SIZE) {
        val tpr: Float = tempValues.TP// / (tempValues.TP + tempValues.FN)
        val fpr: Float = tempValues.FP// / (tempValues.FP + tempValues.TN)
        rates += Rate(tpr, fpr)
        j = 0
      } else {
        j += 1
      }
    })

    val map: ListBuffer[(Float, Float)] = rates.map(rate => (rate.TPR / rates.last.TPR, rate.FPR / rates.last.FPR))
    println("TPR")
    map.foreach(element => println(element._1))
    println("FPR")
    map.foreach(element => println(element._2))
  }

  private case class Tuple(expected : Float, probabilityHigh : Float, probabilityLow : Float, actual : Float)
  private case class Rate(TPR : Float, FPR : Float)
  private case class Values(var TP : Float, var FP : Float, var TN : Float, var FN : Float)
}
