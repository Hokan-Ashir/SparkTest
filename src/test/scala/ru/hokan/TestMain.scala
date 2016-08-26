package ru.hokan

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class TestMain extends FlatSpec with GivenWhenThen with Matchers {

  "Average data from same ips" should "be computed" in {
    Given("one ip")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Some Application")
    val sc = SparkContext.getOrCreate(conf)
    val values = Array(("ip1", 10L), ("ip1", 20L))
    val rdd: RDD[(String, Long)] = sc.parallelize(values)

    When("count ip average data")
    val result = HW1.countAverageByteNumberValues(rdd)

    Then("average data counted")
    result should equal (Array("ip1, 15.0, 30.0"))
  }

  "Average data from separate different ips" should "be computed" in {
    Given("multiple ip")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Some Application")
    val sc = SparkContext.getOrCreate(conf)
    val values = Array(("ip1", 10L), ("ip2", 20L))
    val rdd: RDD[(String, Long)] = sc.parallelize(values)

    When("count ip average data")
    val result = HW1.countAverageByteNumberValues(rdd)

    Then("average data counted")
    result should equal (Array("ip2, 20.0, 20.0", "ip1, 10.0, 10.0"))
  }

  "Average data from connected different ips" should "be computed" in {
    Given("multiple ip")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Some Application")
    val sc = SparkContext.getOrCreate(conf)
    val values = Array(("ip1", 10L), ("ip2", 20L), ("ip1", 30L), ("ip2", 40L))
    val rdd: RDD[(String, Long)] = sc.parallelize(values)

    When("count ip average data")
    val result = HW1.countAverageByteNumberValues(rdd)

    Then("average data counted")
    result should equal (Array("ip2, 30.0, 60.0", "ip1, 20.0, 40.0"))
  }

  "Average data from separate different ips" should "NOT be equal to simple string" in {
    Given("multiple ip")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Some Application")
    val sc = SparkContext.getOrCreate(conf)
    val values = Array(("ip1", 10L), ("ip2", 20L))
    val rdd: RDD[(String, Long)] = sc.parallelize(values)

    When("count ip average data")
    val result = HW1.countAverageByteNumberValues(rdd)

    Then("average data counted")
    result should not equal Array("ip1")
  }

  "Average data from separate different ips" should "NOT be equal to string with wrong sorting order" in {
    Given("multiple ip")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Some Application")
    val sc = SparkContext.getOrCreate(conf)
    val values = Array(("ip1", 10L), ("ip2", 20L))
    val rdd: RDD[(String, Long)] = sc.parallelize(values)

    When("count ip average data")
    val result = HW1.countAverageByteNumberValues(rdd)

    Then("average data counted")
    result should not equal Array("ip1, 10.0, 10.0", "ip2, 20.0, 20.0")
  }
}
