package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggDemo extends Serializable {
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

        invoiceDF.select(
          count("*").as("Count *"),
          sum("Quantity").as("TotalQuantity"),
          avg("UnitPrice").as("AvgPrice"),
          countDistinct("InvoiceNo").as("CountDistinct")
        ).show()


        invoiceDF.selectExpr(
          "count(1) as `count 1`",
          "count(StockCode) as `count field`",
          "sum(Quantity) as TotalQuantity",
          "avg(UnitPrice) as AvgPrice"
        ).show()

        invoiceDF.createOrReplaceTempView("sales")
        val summarySQL = spark.sql(
          """
            |SELECT Country, InvoiceNo,
            | sum(Quantity) as TotalQuantity,
            | round(sum(Quantity*UnitPrice),2) as InvoiceValue
            | FROM sales
            | GROUP BY Country, InvoiceNo
            |""".stripMargin)

        summarySQL.show()

    val summaryDF = invoiceDF.groupBy("Country", "InvoiceNo")
      .agg(sum("Quantity").as("TotalQuantity"),
        round(sum(expr("Quantity*UnitPrice")), 2).as("InvoiceValue"),
        expr("round(sum(Quantity*UnitPrice),2) as InvoiceValueExpr")
      )
    summaryDF.show()

  }

}
