package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object JoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Join Demo")
      .master("local[3]")
      .getOrCreate()

    val flightTimeDF1 = spark.read.json("data/d1/")
    val flightTimeDF2 = spark.read.json("data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    val joinExpr = flightTimeDF1.col("id") === flightTimeDF2.col("id")

    val joinDF = flightTimeDF1.join(flightTimeDF2, joinExpr, "inner")

    joinDF.foreach(_ => ())
    scala.io.StdIn.readLine()
  }

}
