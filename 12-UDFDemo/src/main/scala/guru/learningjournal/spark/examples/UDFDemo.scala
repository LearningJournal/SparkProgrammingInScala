package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object UDFDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.error("Usage: UDFDemo filename")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("UDF Demo")
      .master("local[3]")
      .getOrCreate()

    val surveyDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))

    surveyDF.show(10,false)
    import org.apache.spark.sql.functions._
    val parseGenderUDF = udf(parseGender(_:String):String)
    spark.catalog.listFunctions().filter(r => r.name == "parseGenderUDF").show()
    val surveyDF2 = surveyDF.withColumn("Gender", parseGenderUDF(col("Gender")))
    surveyDF2.show(10,false)

    spark.udf.register("parseGenderUDF", parseGender(_:String):String)
    spark.catalog.listFunctions().filter(r => r.name == "parseGenderUDF").show()
    val surveyDF3 = surveyDF.withColumn("Gender", expr("parseGenderUDF(Gender)"))
    surveyDF3.show(10, false)
  }

  def parseGender(s:String):String = {
    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.l".r

    if(femalePattern.findFirstIn(s.toLowerCase).nonEmpty) "Female"
    else if (malePattern.findFirstIn(s.toLowerCase).nonEmpty) "Male"
    else "Unknown"
  }

}
