package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GroupingDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Misc Demo")
      .master("local[3]")
      .getOrCreate()

    val invoiceDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/invoices.csv")

    val NumInvoices = countDistinct("InvoiceNo").as("NumInvoices")
    val TotalQuantity = sum("Quantity").as("TotalQuantity")
    val InvoiceValue = expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    val exSummaryDF = invoiceDF
      .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm"))
      .where("year(InvoiceDate) == 2010")
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
      .groupBy("Country", "WeekNumber")
      .agg(NumInvoices, TotalQuantity, InvoiceValue)

    exSummaryDF.coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save("output")

    exSummaryDF.sort("Country", "WeekNumber").show()
  }
}
