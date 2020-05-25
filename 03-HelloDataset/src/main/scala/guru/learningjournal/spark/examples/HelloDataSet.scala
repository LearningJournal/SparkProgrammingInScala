package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object HelloDataSet extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: HelloDataSet filename")
      System.exit(1)
    }

    //Create Spark Session
    val spark = SparkSession.builder()
      .appName("Hello DataSet")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._


    //Read your CSV file
    val rawDF:Dataset[Row] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    //Type Safe Data Set
    val surveyDS:Dataset[SurveyRecord] = rawDF.select("Age", "Gender", "Country", "state").as[SurveyRecord]

    //Type safe Filter
    val filteredDS = surveyDS.filter(r => r.Age < 40)
    //Runtime Filter
    val filteredDF = surveyDS.filter("Age  < 40")

    //Type safe GroupBy
    val countDS = filteredDS.groupByKey(r => r.Country).count()
    //Runtime GroupBy
    val countDF = filteredDF.groupBy("Country").count()

    logger.info("DataFrame: " + countDF.collect().mkString(","))
    logger.info("DataSet: " + countDS.collect().mkString(","))

    //Uncomment if you want to investigate SparkUI
    //scala.io.StdIn.readLine()
    spark.stop()
  }

}
