package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object WindowingDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Windowing Demo")
      .master("local[3]")
      .getOrCreate()

    val summaryDF = spark.read.parquet("data/summary.parquet")

    val runningTotalWindow = Window.partitionBy("Country")
      .orderBy("WeekNumber")
      .rowsBetween(-2, Window.currentRow)

    summaryDF.withColumn("RunningTotal",
      sum("InvoiceValue").over(runningTotalWindow)
    ).show()

  }

}
