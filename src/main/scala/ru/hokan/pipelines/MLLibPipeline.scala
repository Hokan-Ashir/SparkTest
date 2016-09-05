package ru.hokan.pipelines

import org.apache.spark.sql.DataFrame
import ru.hokan.pipelines.mllib.classifiers.{DecisionTreeClassifier, LogisticRegressionLBFGSClassifier, LogisticRegressionSGDClassifier}
import ru.hokan.pipelines.mllib.MLLibDataPreparator

object MLLibPipeline extends ExecutionPipeline{
  override def execute(dataFrame: DataFrame, trainingDataRatio : Double, testDataRatio : Double): Unit = {

    println("MLLibPipeline execution launch")
    println("Data preparation for MLLibPipeline")
    val (trainingData, testData) = MLLibDataPreparator.prepareData(dataFrame, trainingDataRatio, testDataRatio)

    println("Run training data (DecisionTreeClassifier)")
    DecisionTreeClassifier.classify(trainingData, testData)

    println("Run training data (LogisticRegressionSGDClassifier)")
    LogisticRegressionSGDClassifier.classify(trainingData, testData)

    println("Run training data (LogisticRegressionLBFGSClassifier)")
    LogisticRegressionLBFGSClassifier.classify(trainingData, testData)
  }
}
