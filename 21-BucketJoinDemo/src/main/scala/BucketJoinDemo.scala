import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object BucketJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bucket Join Demo")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()

/*
        val df1 = spark.read.json("data/d1/")
        val df2 = spark.read.json("data/d2/")
        //df1.show()
        //df2.show()

        import spark.sql
        sql("CREATE DATABASE IF NOT EXISTS MY_DB")
        sql("USE MY_DB")

        df1.coalesce(1).write
          .bucketBy(3, "id")
          .mode(SaveMode.Overwrite)
          .saveAsTable("MY_DB.flight_data1")

        df2.coalesce(1).write
          .bucketBy(3, "id")
          .mode(SaveMode.Overwrite)
          .saveAsTable("MY_DB.flight_data2")

*/
    val df3 = spark.read.table("MY_DB.flight_data1")
    val df4 = spark.read.table("MY_DB.flight_data2")

    val joinExpr = df3.col("id") === df4.col("id")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val joinDF = df3.join(df4, joinExpr, "inner")

    joinDF.foreach(_ => ())
    scala.io.StdIn.readLine()


  }
}
