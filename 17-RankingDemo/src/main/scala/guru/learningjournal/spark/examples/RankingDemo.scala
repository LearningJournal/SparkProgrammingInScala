package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RankingDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Ranking Demo")
      .master("local[3]")
      .getOrCreate()

    val summaryDF = spark.read.parquet("data/summary.parquet")

    summaryDF.sort("Country", "WeekNumber").show()

    val rankWindow = Window.partitionBy("Country")
      .orderBy(col("InvoiceValue").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summaryDF.withColumn("Rank", dense_rank().over(rankWindow))
      .where(col("Rank") ===1)
      .sort("Country", "WeekNumber")
      .show()

    spark.stop()
  }
}
