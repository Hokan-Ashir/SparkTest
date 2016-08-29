package ru.hokan

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HW2 {

  val DATABASE_NAME = "homework1"
  val AIRPORTS_TABLE_NAME = "airports"
  val DATA_TABLE_NAME = "data"
  val CARRIERS_TABLE_NAME = "carriers"
  val ACCESS_DELIMITER = "."

  def main(args: Array[String]): Unit = {
    val APPLICATION_NAME = "HW2 Application"

    val conf = new SparkConf().setMaster("local[*]").setAppName(APPLICATION_NAME)
    val sc = new SparkContext(conf)
    val context: HiveContext = new HiveContext(sc)

    val carriersTable: DataFrame = context.table(DATABASE_NAME + ACCESS_DELIMITER + CARRIERS_TABLE_NAME)
    carriersTable.show()

    val dataTable: DataFrame = context.table(DATABASE_NAME + ACCESS_DELIMITER + DATA_TABLE_NAME)
    dataTable.show()

    val airportsTable: DataFrame = context.table(DATABASE_NAME + ACCESS_DELIMITER + AIRPORTS_TABLE_NAME)
    airportsTable.show()
  }
}
