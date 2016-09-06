package ru.hokan.pipelines.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}

object LogisticRegressionClassifier {

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

    val fullPredictions = pipelineModel.transform(testData).cache()
    //given input columns prediction, probability, result, features, rawPrediction;
    fullPredictions.select(MLDataPreparator.RESULT_COLUMN, "probability", "rawPrediction", "prediction").show(false)

    val scoreAndLabels = fullPredictions.select("rawPrediction", MLDataPreparator.RESULT_COLUMN)
      .map { case Row(rawPrediction: Vector, label: Double) =>
        (rawPrediction(1), label)
      }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    println(s"Area under ROC vie BCM = ${metrics.areaUnderROC()}")
  }
}
