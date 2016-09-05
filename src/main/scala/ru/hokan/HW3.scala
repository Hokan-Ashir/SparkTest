package ru.hokan

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ru.hokan.pipelines.MLPipeline

object HW3 {

  val DATABASE_NAME = "spark_homework3"
  val ACCESS_DELIMITER = "."
  val OBJECTS_TABLE_NAME = "objects"
  val TARGET_TABLE_NAME = "target"

  val TRAINING_DATA_RATIO: Double = 0.9
  val TEST_DATA_RATIO: Double = 0.1

  def main(args: Array[String]): Unit = {
    val APPLICATION_NAME = "HW3 Application"

    println("Initializing context")
    val conf = new SparkConf().setMaster("local[*]").setAppName(APPLICATION_NAME)
    val sc = new SparkContext(conf)
    val context: HiveContext = new HiveContext(sc)

    println("Uploading data")
    val objectsTable: DataFrame = context.table(DATABASE_NAME + ACCESS_DELIMITER + OBJECTS_TABLE_NAME)
    val targetTable: DataFrame = context.table(DATABASE_NAME + ACCESS_DELIMITER + TARGET_TABLE_NAME)

    println("Joining data")
    val cache: DataFrame = targetTable.join(objectsTable, targetTable("row_num") === objectsTable("row_num"))
//    .select(
//      targetTable("result"),
//      objectsTable("age"),
//      objectsTable("personal_income"),
//      objectsTable("city_of_registration"),
//      objectsTable("city_of_living"),
//      objectsTable("term_of_loan"),
//      objectsTable("family_income")//,
//      objectsTable("number_of_closed_loans")
//      objectsTable("direction_of_activity_inside_company")
//    )
    .cache()

//    MLLibPipeline.execute(cache, TRAINING_DATA_RATIO, TEST_DATA_RATIO)
    MLPipeline.execute(cache, TRAINING_DATA_RATIO, TEST_DATA_RATIO)
  }
}
