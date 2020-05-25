package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class ApacheLogRecord(ip: String, date: String, request: String, referrer: String)

object LogFileDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Log File Demo")
      .master("local[3]")
      .getOrCreate()

    val logsDF = spark.read.textFile("data/apache_logs.txt").toDF()

    val myReg = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r

    import spark.implicits._

    val logDF = logsDF.map(row =>
      row.getString(0) match {
        case myReg(ip, client, user, date, cmd, request, proto, status, bytes, referrer, userAgent) =>
          ApacheLogRecord(ip, date, request, referrer)
      }
    )

    import org.apache.spark.sql.functions._
    logDF
      .where("trim(referrer) != '-' ")
      .withColumn("referrer", substring_index($"referrer", "/", 3))
      .groupBy("referrer").count().show(truncate = false)
    spark.stop()
  }
}
