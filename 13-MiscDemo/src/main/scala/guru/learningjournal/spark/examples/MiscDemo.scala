package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object MiscDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Misc Demo")
      .master("local[3]")
      .getOrCreate()

    val dataList = List(
      ("Ravi", "28", "1", "2002"),
      ("Abdul", "23", "5", "81"), // 1981
      ("John", "12", "12", "6"), // 2006
      ("Rosy", "7", "8", "63"), // 1963
      ("Abdul", "23", "5", "81")
    )

    val rawDF = spark.createDataFrame(dataList).toDF("name", "day", "month", "year").repartition(3)
    //rawDF.printSchema()

    //case when year < 21 then cast(year as int) + 2000
    //when year < 100 then cast(year as int) + 1900

    val finalDF = rawDF.withColumn("id", monotonically_increasing_id)
      .withColumn("day", col("day").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("year", col("year").cast(IntegerType))
      /*
      .withColumn("year", expr(
        """
          |case when year < 21 then year + 2000
          |when year < 100 then year + 1900
          |else year
          |end
          |""".stripMargin))
      */
      .withColumn("year",
        when(col("year") < 21, col("year") + 2000)
          when(col("year") < 100, col("year") + 1900)
          otherwise (col("year"))
      )
       // .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
      .withColumn("dob", to_date(expr("concat(day,'/',month,'/',year)"),"d/M/y"))
        .drop("day", "month", "year")
        .dropDuplicates("name", "dob")
        .sort(expr("dob desc"))
    finalDF.show


  }

}
