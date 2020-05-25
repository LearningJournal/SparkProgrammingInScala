package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object HelloSparkSQL extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: HelloSparkSQL filename")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Hello Spark SQL")
      .master("local[3]")
      .getOrCreate()

    val surveyDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    surveyDF.createOrReplaceTempView("survey_tbl")

    val countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    logger.info(countDF.collect().mkString("->"))
    //scala.io.StdIn.readLine()
    spark.stop()

  }
}
