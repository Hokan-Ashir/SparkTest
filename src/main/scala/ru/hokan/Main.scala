package ru.hokan

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.immutable.HashMap

object Main {

  val FILE_PATH = "hdfs:///user/maria_dev/"
  val FILE_NAME = "000000"
  val APPLICATION_NAME: String = "Simple Application"
  val REGEX_PATTERN = "(ip\\d+)\\s-\\s-\\s\\[.*\\]\\s\".*\"\\s\\d+\\s(\\d+)?.*".r
  val PARTS_FILE_NAME_PREFIX: String = "temp"
  val RESULT_FILE_NAME: String = "result.csv"
  val userAgentParser : UserAgentParser = new UserAgentParser
  var accumulators : Map[BrowserTypes.BrowserTypes, Accumulator[Int]] = null

  def main(args: Array[String]) {
    val (sc : SparkContext, cache: RDD[(String, Long)]) = extractIPAndBytesAmount
    val map: Array[String] = countAverageByteNumberValues(cache)
    map.foreach(println)
    accumulators.foreach(println)
    saveResultToFile(sc, map)
  }

  def extractIPAndBytesAmount: (SparkContext, RDD[(String, Long)]) = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(APPLICATION_NAME)
    val sc = new SparkContext(conf)
    accumulators = HashMap(
      (BrowserTypes.IE, sc.accumulator(0)),
      (BrowserTypes.MOZILLA, sc.accumulator(0)),
      (BrowserTypes.OTHER, sc.accumulator(0))
    )

    val cache = sc.textFile(FILE_PATH + FILE_NAME, 1).map(line => {
      val REGEX_PATTERN(ip, size) = line
      val accumulator: Accumulator[Int] = accumulators(userAgentParser.getBrowserType(line))
      accumulator += 1
      (ip, if (size != null) size.toLong else 0)
    })

    (sc, cache)
  }

  def saveResultToFile(sc: SparkContext, map: Array[String]): Unit = {
    val partFilesPath: String = FILE_PATH + PARTS_FILE_NAME_PREFIX
    val resultFilePath: String = FILE_PATH + RESULT_FILE_NAME

    FileUtil.fullyDelete(new File(partFilesPath))
    FileUtil.fullyDelete(new File(resultFilePath))

    sc.parallelize(map).saveAsTextFile(partFilesPath)
    merge(partFilesPath, resultFilePath)
  }

  def countAverageByteNumberValues(cache: RDD[(String, Long)]): Array[String] = {
    val key: RDD[(String, (Float, Int))] = cache.aggregateByKey((0.0f, 0))(
      (acc, size) => (acc._1 + size, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    val map: Array[String] = key.mapValues {
      case (totalAmountOfBytes, amountOfRecords) =>
        (totalAmountOfBytes / amountOfRecords, totalAmountOfBytes)
    }.sortBy({
      case (_, (_, totalAmountOfBytes)) => totalAmountOfBytes
    }, ascending = false)
      .take(5)
      .map {
        case (ipName, (averageValue, totalAmountOfBytes)) =>
          ipName + ", " + averageValue + ", " + totalAmountOfBytes
      }
    map
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }
}