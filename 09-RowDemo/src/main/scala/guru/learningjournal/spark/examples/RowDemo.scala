package guru.learningjournal.spark.examples

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RowDemo extends Serializable{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Row Demo")
      .master("local[3]")
      .getOrCreate()

    val mySchema = StructType(List(
      StructField("ID", StringType),
      StructField("EventDate", StringType)))

    val myRows = List(Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("125", "4/05/2020"))
    val myRDD = spark.sparkContext.parallelize(myRows, 2)
    val myDF = spark.createDataFrame(myRDD, mySchema)

    myDF.printSchema
    myDF.show
    val newDF = toDateDF(myDF,  "M/d/y", "EventDate")
    newDF.printSchema
    newDF.show

    spark.stop()
  }

  def toDateDF(df:DataFrame, fmt:String, fld:String):DataFrame = {
    df.withColumn(fld, to_date(col(fld),fmt))
  }

}
