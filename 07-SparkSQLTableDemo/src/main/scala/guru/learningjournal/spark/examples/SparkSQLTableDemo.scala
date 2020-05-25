package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQLTableDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark SQL Table Demo")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "dataSource/")
      .load()

    import spark.sql
    sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    sql("USE MY_DB")

    flightTimeParquetDF.write
      .mode(SaveMode.Overwrite)
      //.partitionBy("ORIGIN", "OP_CARRIER")
      .bucketBy(5, "OP_CARRIER", "ORIGIN")
      .sortBy("ORIGIN", "OP_CARRIER")
      .saveAsTable("MY_DB.flight_data")

    spark.catalog.listTables("MY_DB").show()

    logger.info("Finished.")
    spark.stop()
  }

}
