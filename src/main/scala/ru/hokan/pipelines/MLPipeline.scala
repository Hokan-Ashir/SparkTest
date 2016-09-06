package ru.hokan.pipelines
import org.apache.spark.sql.DataFrame
import ru.hokan.pipelines.ml.{LogisticRegressionClassifier, MLDataPreparator}

object MLPipeline extends ExecutionPipeline{
  override def execute(dataFrame: DataFrame, trainingDataRatio: Double, testDataRatio: Double): Unit = {

    println("MLPipeline execution launch")
    println("Data preparation for MLPipeline")
    val (trainingData, testData) = MLDataPreparator.prepareData(dataFrame, trainingDataRatio, testDataRatio)

    println("Run training data (LogisticRegressionClassifier)")
    LogisticRegressionClassifier.classify(trainingData, testData)
  }
}
