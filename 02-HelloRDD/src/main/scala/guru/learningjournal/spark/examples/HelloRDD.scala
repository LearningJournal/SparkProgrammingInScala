package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object HelloRDD extends Serializable {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    if (args.length == 0) {
      logger.info("Usage: HelloRDD filename")
      System.exit(1)
    }

    //Create Spark Context
    val sparkAppConf = new SparkConf().setAppName("HelloRDD").setMaster("local[3]")
    val sparkContext = new SparkContext(sparkAppConf)

    //Read your CSV file
    val linesRDD = sparkContext.textFile(args(0),2)

    //Give it a Structure and select only 4 columns
    case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)
    val colsRDD = linesRDD.map(line => {
      val cols = line.split(",").map(_.trim)
      SurveyRecord(cols(1).toInt, cols(2), cols(3), cols(4))
    })

    //Apply Filter
    val filteredRDD = colsRDD.filter(r => r.Age < 40)

    //Manually implement the GroupBy
    val kvRDD = filteredRDD.map(r => (r.Country, 1))
    val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)

    //Collect the result
    logger.info(countRDD.collect().mkString(","))

    //Stop the Spark context
    //scala.io.StdIn.readLine()
    sparkContext.stop()
  }

}
