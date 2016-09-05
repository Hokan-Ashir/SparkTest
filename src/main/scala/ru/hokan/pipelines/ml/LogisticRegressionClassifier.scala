package ru.hokan.pipelines.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, OneHotEncoder, StringIndexer, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.util.Vector

object LogisticRegressionClassifier {
  def classify(trainingData: DataFrame, testData : DataFrame) : Unit = {

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
//    val tokenizer = new Tokenizer()
//      .setInputCol("text")
//      .setOutputCol("words")
//    val hashingTF = new HashingTF()
//      .setInputCol(tokenizer.getOutputCol)
//      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val pipeline = new Pipeline()
//      .setStages(Array(tokenizer, hashingTF, lr))
      .setStages(Array(lr))

//    val indexer = new StringIndexer()
//      .setInputCol("family_income")
//      .setOutputCol("family_income_Index")
//      .fit(trainingData)
//    val indexed = indexer.transform(trainingData)
//
//    val encoder = new OneHotEncoder()
//      .setInputCol("family_income_Index")
//      .setOutputCol("family_income_Vector")
//    val transform: DataFrame = encoder.transform(indexed)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
//      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(trainingData)
//    cvModel.getEvaluator.asInstanceOf[BinaryClassificationEvaluator].

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
//    cvModel.transform(testData)
//      .select("id", "text", "probability", "prediction")
//      .collect()
//      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
//        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
//      }

//    val trainingSummary = cv.me
//
//    // Obtain the objective per iteration.
//    val objectiveHistory = trainingSummary.objectiveHistory
//    objectiveHistory.foreach(loss => println(loss))
//
//    // Obtain the metrics useful to judge performance on test data.
//    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
//    // binary classification problem.
//    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
//
//    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
//    val roc = binarySummary.roc
//    roc.show()
//    println(binarySummary.areaUnderROC)
  }
}
