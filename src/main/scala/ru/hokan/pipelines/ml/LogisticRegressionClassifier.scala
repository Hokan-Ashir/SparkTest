package ru.hokan.pipelines.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
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
      .setMaxIter(100)

    stages += lor
    val pipeline = new Pipeline().setStages(stages.toArray)

    val paramGrid = new ParamGridBuilder()
//      .addGrid(lor.regParam, Array(0.1, 0.01))
//      .addGrid(lor.maxIter, Array(10, 100, 1000))
//        .addGrid(lor.elasticNetParam, Array(0.1, 0.01))
//        .addGrid(lor.tol, Array(0.1, 0.01, 0.001, 0.0001, 0.00001))
        .addGrid(lor.standardization, Array(x = false))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)  // Use 3+ in practice

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipelineModel = cv.fit(trainingData)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

//    val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
    // Print the weights and intercept for logistic regression.
//    println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")

    val fullPredictions = pipelineModel.transform(testData).cache()
    fullPredictions.printSchema()
    //given input columns prediction, probability, result, features, rawPrediction;
    val selectResult: DataFrame = fullPredictions.select(MLDataPreparator.RESULT_COLUMN, "probability", "rawPrediction", "prediction")
    selectResult.show(false)
    selectResult.rdd.saveAsTextFile("/opt/result.txt")

    val scoreAndLabels = fullPredictions.select("rawPrediction", MLDataPreparator.RESULT_COLUMN)
      .map { case Row(rawPrediction: Vector, label: Double) =>
        (rawPrediction(1), label)
      }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    println(s"Area under ROC vie BCM = ${metrics.areaUnderROC()}")
  }
}
