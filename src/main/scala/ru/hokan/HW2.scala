package ru.hokan

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

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
//    carriersTable.show()
    val dataTable: DataFrame = context.table(DATABASE_NAME + ACCESS_DELIMITER + DATA_TABLE_NAME)
//    dataTable.show()
    val airportsTable: DataFrame = context.table(DATABASE_NAME + ACCESS_DELIMITER + AIRPORTS_TABLE_NAME)
//    airportsTable.show()

    val query1DF: DataFrame = executeQuery1(carriersTable, dataTable)
    query1DF.show()

    val query2DF: DataFrame = executeQuery2(dataTable, airportsTable)
    query2DF.show()

    val query3DF: DataFrame = executeQuery3(dataTable, airportsTable)
    query3DF.show()

    val query4DF: DataFrame = executeQuery4(carriersTable, dataTable)
    query4DF.show()
  }

  //  --Count total number of flights per carrier in 2007
  //  select result.cnt, carriers.code, carriers.description from
  //    (select count(1) as cnt, uniquecarrier from data group by uniquecarrier order by cnt desc) as result
  //  join
  //  carriers
  //  on result.UniqueCarrier = carriers.code;
  def executeQuery1(carriersTable: DataFrame, dataTable: DataFrame): DataFrame = {
    getGroupedUniqueCarriersCount(dataTable)
      .join(carriersTable, dataTable("UniqueCarrier") === carriersTable("code")).select("cnt", "code", "description")
  }

  //  --The total number of flights served in Jun 2007 by NYC (all airports, use join with Airports data)
  //  select count(1) from (
  //    select * from data JOIN airports on (data.Origin = airports.iata)
  //      where data.Month = 6 and airports.city = 'New York'
  //  UNION
  //  select * from data JOIN airports on (data.Dest = airports.iata)
  //  where data.Month = 6 and airports.city = 'New York'
  //  ) as result;
  def executeQuery2(dataTable: DataFrame, airportsTable: DataFrame): DataFrame = {
    dataTable.join(airportsTable, dataTable("Origin") === airportsTable("iata")).where("Month = 6 and city = 'New York'")
    .unionAll(dataTable.join(airportsTable, dataTable("Dest") === airportsTable("iata")).where("Month = 6 and city = 'New York'"))
      .agg(count("*").alias("cnt")).select("cnt")
  }

  //  --Find five most busy airports in US during Jun 01 - Aug 31
  //  --TODO should additional check in where be performed? country == USA
  //  select count(1) as cnt, result.iata from (
  //    select * from data JOIN airports on (data.Origin = airports.iata)
  //      where data.Month >= 6 and data.Month <= 8
  //      UNION
  //      select * from data JOIN airports on (data.Dest = airports.iata)
  //      where data.Month >= 6 and data.Month <= 8
  //    ) as result
  //  group by result.iata
  //  order by cnt desc limit 5;

  def executeQuery3(dataTable: DataFrame, airportsTable: DataFrame): DataFrame = {
    dataTable.join(airportsTable, dataTable("Origin") === airportsTable("iata")).where("Month >= 6 and Month <= 8")
      .unionAll(dataTable.join(airportsTable, dataTable("Dest") === airportsTable("iata")).where("Month >= 6 and Month <= 8")).
      groupBy("iata").agg(count("*").alias("cnt")).orderBy(desc("cnt")).limit(5)
        .select("cnt", "iata")
  }

  //  --Find the carrier who served the biggest number of flights
  //  select result.cnt, carriers.code, carriers.description from
  //    (select count(1) as cnt, uniquecarrier from data group by uniquecarrier order by cnt desc limit 1) as result
  //    join
  //  carriers
  //  on result.UniqueCarrier = carriers.code;
  def executeQuery4(carriersTable: DataFrame, dataTable: DataFrame): DataFrame = {
    getGroupedUniqueCarriersCount(dataTable).limit(1)
      .join(carriersTable, dataTable("UniqueCarrier") === carriersTable("code")).select("cnt", "code", "description")
  }

  def getGroupedUniqueCarriersCount(dataTable: DataFrame): DataFrame = {
    dataTable.select("UniqueCarrier").groupBy("UniqueCarrier").agg(count("*").alias("cnt")).orderBy(desc("cnt"))
  }
}
