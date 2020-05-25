package guru.learningjournal.spark.examples

import java.sql.Date

import guru.learningjournal.spark.examples.RowDemo.toDateDF
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class myRow(ID: String, EventDate: Date)

class RowDemoTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _
  @transient var myDF: DataFrame = _

  override def beforeAll() {
    spark = SparkSession.builder()
      .appName("Demo Row Test")
      .master("local[3]")
      .getOrCreate()

    val mySchema = StructType(List(
      StructField("ID", StringType),
      StructField("EventDate", StringType)))

    val myRows = List(Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("125", "4/05/2020"))
    val myRDD = spark.sparkContext.parallelize(myRows, 2)
    myDF = spark.createDataFrame(myRDD, mySchema)

  }

  override def afterAll() {
    spark.stop()
  }

  test("Test Data Type") {

    val rowList = toDateDF(myDF, "M/d/y", "EventDate").collectAsList()
    rowList.forEach(r =>
      assert(r.get(1).isInstanceOf[Date], "Second column should be Date")
    )

  }

  test("Test Date Value") {
    val spark2 = spark
    import spark2.implicits._

    val rowList = toDateDF(myDF, "M/d/y", "EventDate").as[myRow].collectAsList()

    rowList.forEach(r =>
      assert(r.EventDate.toString == "2020-04-05", "Date string must be 2020-04-05")
    )
  }

}
