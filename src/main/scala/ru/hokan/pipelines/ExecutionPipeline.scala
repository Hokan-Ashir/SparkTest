package ru.hokan.pipelines

import org.apache.spark.sql.DataFrame

trait ExecutionPipeline {
  def execute(dataFrame: DataFrame, trainingDataRatio : Double, testDataRatio : Double) : Unit
}
