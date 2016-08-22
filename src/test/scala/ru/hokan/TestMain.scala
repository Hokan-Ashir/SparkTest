package ru.hokan

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class TestMain extends FlatSpec with GivenWhenThen with Matchers {

  "Shakespeare most famous quote" should "be counted" in {
    Given("quote")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Some Application")
    val sc = SparkContext.getOrCreate(conf)
    val values = Array(("ip1", 10L), ("ip1", 20L))
    val rdd: RDD[(String, Long)] = sc.parallelize(values)

    When("count words")
    val wordCounts = Main.executeComputations(rdd)

    Then("words counted")
    wordCounts should equal (Array("ip1, 15.0, 30.0"))
  }
}
