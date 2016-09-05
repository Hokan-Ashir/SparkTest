package ru.hokan.pipelines.ml

import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.sql.DataFrame

object MLDataPreparator {
  val FEATURES_COLUMN = "features"
  val RESULT_COLUMN = "result"
  val ROW_NUMBER = "row_num"
  val CATEGORY_FEATURES = Array("is_employed", "is_retired", "sex", "education", "marital_status",
    "branch_of_employment",
    "position_in_company", "company_ownership", "relationship_to_foreign_capital",
  "direction_of_activity_inside_company", "family_income", "city_of_registration", "city_of_living",
    "postal_address_of_city", "city_of_branch_loan_was_taken", "state")
  val RESULT_COLUMNS = Array(RESULT_COLUMN, FEATURES_COLUMN)
  val NON_FEATURES_COLUMNS = Array(ROW_NUMBER, RESULT_COLUMN)

  val NON_CATEGORIZED_VALUE: Int = 31

  def prepareData(dataFrame: DataFrame, trainingDataRatio : Double, testDataRatio : Double): (DataFrame, DataFrame) = {

    val filter: Array[String] = dataFrame.schema.fields.map(field => field.name).filter(name => !name.contains(ROW_NUMBER))
//    filter.foreach(println)
    val dfWithoutRowNumberJoinCoulumn = dataFrame.select(filter.head, filter.tail:_*)

    val dfWithoutNaNs: DataFrame = dfWithoutRowNumberJoinCoulumn.na.fill(NON_CATEGORIZED_VALUE)
    val dfWithOHEProcessed: DataFrame = transformCategoricalFeatures(dfWithoutNaNs)
//    dfWithOHEProcessed.printSchema()
//    dfWithOHEProcessed.take(17).foreach(println)

    val vectorAssembledColumns: Array[String] = getVectorAssembledColumns(dfWithOHEProcessed)
    val assembler: VectorAssembler = new VectorAssembler().setInputCols(vectorAssembledColumns).setOutputCol(FEATURES_COLUMN)
    val dfWithProcessedFeatures: DataFrame = assembler.transform(dfWithOHEProcessed)
    val dfWithFilteredProcessedFeatures: DataFrame = dfWithProcessedFeatures.select(RESULT_COLUMNS.head, RESULT_COLUMNS.tail: _*)

//    dfWithFilteredProcessedFeatures.printSchema()
//    dfWithFilteredProcessedFeatures.take(17).foreach(println)

    println("Splitting training data")
    val splits = dfWithFilteredProcessedFeatures.randomSplit(Array(trainingDataRatio, testDataRatio), seed = 11L)
    val trainingData = splits(0).cache()
    val testData = splits(1)

    (trainingData, testData)
  }

  def transformCategoricalFeatures(dataFrame: DataFrame) : DataFrame = {
    var result: DataFrame = dataFrame
    for (name <- CATEGORY_FEATURES) {
      result = transformCategoricalFeature(result, name)
    }

    val allColumnNames: Array[String] = result.schema.fields.map(field => field.name)
    val nonCategoryColumns: Array[String] = allColumnNames diff CATEGORY_FEATURES
    result.select(nonCategoryColumns.head, nonCategoryColumns.tail: _*)
  }

  def transformCategoricalFeature(dataFrame: DataFrame, name : String) : DataFrame = {
    val encoder = new OneHotEncoder()
      .setInputCol(name)
      .setOutputCol(name + "_vectored")
    encoder.transform(dataFrame)
  }

  def getVectorAssembledColumns(dataFrame: DataFrame) : Array[String] = {
    dataFrame.schema.fields.map(field => field.name).filter(name => !NON_FEATURES_COLUMNS.contains(name))
  }
}
