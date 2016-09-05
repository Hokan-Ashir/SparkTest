package ru.hokan.pipelines.ml

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object LogisticRegressionClassifier2 {

  val INDEXED_RESULT_COLUMN_NAME = MLDataPreparator.RESULT_COLUMN + "_" + "indexed"

  def classify(trainingData: DataFrame, testData : DataFrame): Unit = {
    // Set up Pipeline.
    val stages = new scala.collection.mutable.ArrayBuffer[PipelineStage]()

//    val labelIndexer = new StringIndexer()
//      .setInputCol(MLDataPreparator.RESULT_COLUMN)
//      .setOutputCol(INDEXED_RESULT_COLUMN_NAME)
//    stages += labelIndexer

    val lor = new LogisticRegression()
      .setFeaturesCol(MLDataPreparator.FEATURES_COLUMN)
      .setLabelCol(MLDataPreparator.RESULT_COLUMN)
//      .setRegParam(params.regParam) // def: 0.0
//      .setElasticNetParam(params.elasticNetParam) // def 0.0
      .setMaxIter(100)
//      .setTol(params.tol) // 1E-6
//      .setFitIntercept(params.fitIntercept)

    stages += lor
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipelineModel = pipeline.fit(trainingData)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
    // Print the weights and intercept for logistic regression.
    println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")
//    new BinaryClassificationMetrics()

    val fullPredictions = pipelineModel.transform(testData).cache()
    val predictionsAndLabels: RDD[(Double, Double)] = fullPredictions.select("prediction", MLDataPreparator.RESULT_COLUMN).map(row => (row.getDouble(0), row.getDouble(1)))
//    val labels = fullPredictions.select("result").rdd.map(_.getDouble(0))

    val metrics: BinaryClassificationMetrics = new BinaryClassificationMetrics(predictionsAndLabels)
    val c: Double = metrics.areaUnderROC()
    println("Area under ROC: " + c)

//    println("Training data results:")
//    DecisionTreeExample.evaluateClassificationModel(pipelineModel, training, "indexedLabel")
//    println("Test data results:")
//    DecisionTreeExample.evaluateClassificationModel(pipelineModel, test, "indexedLabel")
  }
}
