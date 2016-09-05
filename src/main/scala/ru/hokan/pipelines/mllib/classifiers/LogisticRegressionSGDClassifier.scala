package ru.hokan.pipelines.mllib.classifiers
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object LogisticRegressionSGDClassifier extends Classifier {
  override def classify(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    // Run training algorithm to build the model
    val model: LogisticRegressionModel = LogisticRegressionWithSGD.train(trainingData, 1000, 0.01)
//    val model = new LogisticRegressionWithSGD()
//      .run(trainingData)

    println("Compute results")
    // Compute raw scores on the test set.
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    println("Get evaluation metrics")
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    println("scoreAndLabels")
    val labels: RDD[(Double, Double)] = metrics.scoreAndLabels
    labels.map(rdd => {
      print(rdd._1 + ", " + rdd._2)
    })

    println("roc")
    val roc: RDD[(Double, Double)] = metrics.roc()
    roc.map(rdd => {
      print(rdd._1 + ", " + rdd._2)
    })


    val areaUnderROC = metrics.areaUnderROC()
    println(s"Area Under ROC = $areaUnderROC")
    val trainingError = predictionAndLabels.filter(r => r._1 != r._2).count.toDouble / testData.count
    println("Training error = " + trainingError)
  }
}
