package ru.hokan

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val FILE_PATH = "hdfs:///user/maria_dev/"
  val FILE_NAME = "000000"
  val APPLICATION_NAME: String = "Simple Application"
  val REGEX_PATTERN = "(ip\\d+)\\s-\\s-\\s\\[.*\\]\\s\".*\"\\s\\d+\\s(\\d+)?.*".r
  val PARTS_FILE_NAME_PREFIX: String = "hdfs:///user/maria_dev/temp"
  val RESULT_FILE_NAME: String = "hdfs:///user/maria_dev/result.csv"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(APPLICATION_NAME)
    val sc = new SparkContext(conf)
    val cache = sc.textFile(FILE_PATH + FILE_NAME, 1).map(line => {
      val REGEX_PATTERN(ip, size) = line
      (ip, if (size != null) size.toLong else 0)
    })

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
    map.foreach(println)

    FileUtil.fullyDelete(new File(PARTS_FILE_NAME_PREFIX))
    FileUtil.fullyDelete(new File(RESULT_FILE_NAME))

    sc.parallelize(map).saveAsTextFile(PARTS_FILE_NAME_PREFIX)
    merge(PARTS_FILE_NAME_PREFIX, RESULT_FILE_NAME)
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }
}