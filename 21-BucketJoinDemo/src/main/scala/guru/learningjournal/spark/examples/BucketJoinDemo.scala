package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object BucketJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Join Demo")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()

    val flightDF1 = spark.read.table("MY_DB.flight_data1")
    val flightDF2 = spark.read.table("MY_DB.flight_data2")

    val joinExpr = flightDF1.col("id") === flightDF2.col("id")

    val joinDF = flightDF1.join(flightDF2, joinExpr, "inner")

    joinDF.show()
    scala.io.StdIn.readLine()


  }

}
