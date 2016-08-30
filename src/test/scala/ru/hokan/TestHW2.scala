package ru.hokan

import java.util

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable
import scala.collection.mutable.MutableList


class TestHW2 extends FunSuite with Matchers {

  case class Airport(iata : String, airport : String, city : String)
  case class Carrier(code : String, description : String)
  case class Data(Month : Int, UniqueCarrier : String, Origin : String, Dest : String)

  private val airports: Seq[Airport] = Seq(
    Airport("AAA", "Domodedovo", "Moscow"),
    Airport("ABA", "Domodedovo-2", "Moscow"),
    Airport("ACA", "Domodedovo-3", "Moscow"),
    Airport("BAA", "Pulkovo-1", "SPB"),
    Airport("BBA", "Pulkovo-2", "SPB"),
    Airport("BCA", "Pulkovo-3", "SPB")
  )

  private val carriers: Seq[Carrier] = Seq(
    Carrier("JUA", "Just Under Acquision"),
    Carrier("JSO", "Just Some Objection"),
    Carrier("OP", "Obligatory Predator"),
    Carrier("DIM", "Dimentional Inner Mage"),
    Carrier("CAU", "Casual Anniversary Utility"),
    Carrier("LKU", "Legit Knot Unifier"),
    Carrier("BYT", "Barbaric Yawn Traitor")
  )

  private val data: Seq[Data] = Seq(
    Data(1, "JUA", "Moscow", "New York"),
    Data(2, "JUA", "Moscow", "New York"),
    Data(3, "JUA", "Moscow", "Not New York"),
    Data(4, "JSO", "Not Moscow", "Not New York"),
    Data(5, "OP", "SPB", "New York"),
    Data(6, "OP", "Not SPB", "New York"),
    Data(7, "JUA", "SPB", "Not New York"),
    Data(8, "CAU", "Not SPB", "Not New York"),
    Data(9, "LKU", "Not even close SPB", "New York"),
    Data(10, "BYT", "Far away from SPB", "New York"),
    Data(11, "DIM", "SPB", "Another New York"),
    Data(12, "BYT", "Not Moscow", "New York")
  )

  val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("Some Application")
  val sc = SparkContext.getOrCreate(conf)
  val context: SQLContext = new SQLContext(sc)
  import context.implicits._
  val airportsTable: DataFrame = airports.toDF
  val carriersTable: DataFrame = carriers.toDF
  val dataTable: DataFrame = data.toDF

  test("Should execute first query (Count total number of flights per carrier in 2007)") {
    val queryDF: DataFrame = HW2.executeQuery1(carriersTable, dataTable)
    val collect: Array[Row] = queryDF.collect()
    val expectedCountSequence = List(4, 2, 2, 1, 1, 1, 1)
    val expectedCodeSequence = List("JUA", "BYT", "OP", "LKU", "CAU", "DIM", "JSO")
    val expectedDescriptionSequence = List(
      "Just Under Acquision",
      "Barbaric Yawn Traitor",
      "Obligatory Predator",
      "Legit Knot Unifier",
      "Casual Anniversary Utility",
      "Dimentional Inner Mage",
      "Just Some Objection")

    val actualCountSeqence : mutable.MutableList[Long] = mutable.MutableList[Long]()
    val actualCodeSeqence : mutable.MutableList[String] = mutable.MutableList[String]()
    val actualDescriptionSeqence : mutable.MutableList[String] = mutable.MutableList[String]()
    collect.foreach(row => {
      actualCountSeqence += row.getAs[Long]("cnt")
      actualCodeSeqence += row.getAs[String]("code")
      actualDescriptionSeqence += row.getAs[String]("description")
    })

    for (i <- actualCodeSeqence.indices) {
      assert(expectedCountSequence(i) == actualCountSeqence(i))
      assert(expectedCodeSequence(i) == actualCodeSeqence(i))
      assert(expectedDescriptionSequence(i) == actualDescriptionSeqence(i))
    }
  }

  test("Should execute second query (The total number of flights served in Jun 2007 by NYC)") {
    val queryDF: DataFrame = HW2.executeQuery2(dataTable, airportsTable)
    val asList: util.List[Row] = queryDF.collectAsList()
    val count: Long = asList.get(0).getAs[Long]("cnt")

    assert(count == 0)
  }

  test("Should execute third query (Find five most busy airports in US during Jun 01 - Aug 31)") {
    val queryDF: DataFrame = HW2.executeQuery3(dataTable, airportsTable)
    val asList: util.List[Row] = queryDF.collectAsList()

    assert(asList.size() == 0)
  }

  test("Should execute forth query (Find the carrier who served the biggest number of flights)") {
    val queryDF: DataFrame = HW2.executeQuery4(carriersTable, dataTable)
    val asList: util.List[Row] = queryDF.collectAsList()
    val count: Long = asList.get(0).getAs[Long]("cnt")
    val code: String = asList.get(0).getAs[String]("code")
    val description: String = asList.get(0).getAs[String]("description")

    assert(count == 4)
    assert(code == "JUA")
    assert(description == "Just Under Acquision")
  }
}
