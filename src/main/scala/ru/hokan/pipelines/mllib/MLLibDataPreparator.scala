package ru.hokan.pipelines.mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object MLLibDataPreparator {

  val categories = Map[String, Int](
    ("family_income", 5),
    ("direction_of_activity_inside_company", 9)
  )

  def prepareData(cache: DataFrame, trainingDataRatio : Double, testDataRatio : Double): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    cache.take(10).foreach(println)
    val limit: DataFrame = cache.agg(count("*").alias("cnt")).select("cnt").limit(1)
    val take: Long = limit.map(row => row.getAs[Long](0)).take(1)(0)

    println("Preparing training data")
    val preparedData: RDD[LabeledPoint] = cache.map { row =>
      val pivot: Int = row.getAs[Int](0)
      var array: ArrayBuffer[Double] = ArrayBuffer()
      for (i <- 1 to 3) {
        val d: Double = row.getAs[Double](i)
        val name: String = row.schema(i).name
        if (categories.contains(name)) {
          val temp: Array[Double] = Array.fill[Double](categories(name))(0)
          temp(d.toInt - 1) = 1
          array = array ++ temp
        } else {
          if (!d.isNaN) {
            array += d
          } else {
            array += 0
          }
        }
      }

      LabeledPoint(pivot, Vectors.dense(array.toArray))
    }

    preparedData.take(10).foreach(println)

    println("Splitting training data")
    val splits = preparedData.randomSplit(Array(trainingDataRatio, testDataRatio), seed = 11L)
    val trainingData = splits(0).cache()
    val testData = splits(1)

    (trainingData, testData)
  }
}
