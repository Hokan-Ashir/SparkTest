package ru.hokan.pipelines.mllib.classifiers

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

trait Classifier {
  def classify(trainingData : RDD[LabeledPoint], testData : RDD[LabeledPoint]) : Unit
}
