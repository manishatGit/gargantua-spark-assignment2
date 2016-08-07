package spark.gargantua

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql._

object GargantuaAssignmentII extends App {

  val spark = SparkSession
    .builder()
    .appName("Gargantua Assignment II")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  import spark.sql

  val df = spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("put/file/path/here")

  //Q1 Different number of Call Types
  val differentCallTypes = df.groupBy("Call Type").count().count()
  println("The different numbers of call types   " + differentCallTypes)
  //Q2 Count of Incidents by Call Type
  val incidentsByCallType = df.select("Incident Number", "Call Type").groupBy("Call Type").count().show(false)

  def getYear(max: String): Int = max.toString.substring(max.toString.length - 4, max.toString.length).toInt

  //Q3 Different number of years
  val minYear = df.select("Call Date").reduce {
    (a, b) =>
      if (getYear(a.getString(0)) < getYear(b.getString(0))) a else b
  }
  val maxYear = df.select("Call Date").reduce {
    (a, b) =>
      if (getYear(a.getString(0)) > getYear(b.getString(0))) a else b
  }
  println(s"The number of years is:  ${getYear(maxYear.getString(0)) - getYear(minYear.getString(0)) + 1}")


  object DateEncoder {
    implicit def dateEncoder = org.apache.spark.sql.Encoders.kryo(classOf[Date])
    implicit def parserEncoder = org.apache.spark.sql.Encoders.kryo(classOf[SimpleDateFormat])
  }

  //Q4 Number of calls in last seven days
  import DateEncoder._

  val latestDate: Date = df.select("Call Date").map {
    case Row(date) => new SimpleDateFormat("MM/dd/yyyy").parse(date.toString)
  }.reduce {
    (date1, date2) => if (date1.getTime() > (date2.getTime())) date1 else date2
  }

  val callsInLastSevenDays = df.select("Call Date").filter {
    date =>
      if (new SimpleDateFormat("MM/dd/yyyy")
        .parse(date.getString(0)).getTime > (new Date(latestDate.getTime - (7 * 24 * 60 * 60 * 1000))).getTime) true
      else
        false
  }.count()
  println(s"Number of calls in last seven days:::: " + callsInLastSevenDays)

  //Q5 SF neighborhood with most calls last year
  df.select("city", "Neighborhood  District", "Call Date").createOrReplaceTempView("table")
  val ndf = sql("select * from table where city='San Francisco'")
    .filter(row => row.getString(2).contains("2015"))
    .groupBy("Neighborhood  District")
    .count()
    .reduce((a, b) => if (a.getLong(1) > b.getLong(1)) a else b)
  ndf match {
    case Row(a, b) => println(s"The neighborhood with max calls in SF: $a with $b calls")
  }
  spark.stop()
}
