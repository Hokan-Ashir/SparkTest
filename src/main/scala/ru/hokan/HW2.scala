package ru.hokan

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HW2 {
  def main(args: Array[String]): Unit = {
    val APPLICATION_NAME = "HW2 Application"

    val conf = new SparkConf().setMaster("local[*]").setAppName(APPLICATION_NAME)
    val sc = new SparkContext(conf)
    val context: HiveContext = new HiveContext(sc)

    val sampleRDD = context.read.orc("hdfs://sandbox.hortonworks.com:9000/apps/hive/warehouse/homework1.db/carriers_orc/000000_0")
    val sample_DF = sampleRDD.toDF()
    val newRDD = sample_DF.rdd
    sample_DF.show()
  }

  case class Carrier(code: String, name: String)

}
