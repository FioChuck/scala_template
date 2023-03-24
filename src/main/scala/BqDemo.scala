import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg // why is this dependency required??

object BqDemo {
  def main(args: Array[String]): Unit = {

    // TODO how to pass arguments in debug? Not possible with Metals?
    val spark = SparkSession.builder
      .appName("Bq Demo")
      // .config("spark.master", "local[*]") // local dev
      // .config(
      //   "spark.hadoop.fs.AbstractFileSystem.gs.impl",
      //   "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
      // )
      // .config("spark.hadoop.fs.gs.project.id", "cf-data-analytics")
      // .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      // .config(
      //   "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
      //   "/Users/chasf/Desktop/cf-data-analytics-f77cb40a9dbd.json"
      // )
      .getOrCreate()

    val df =
      spark.read
        .format("bigquery") // added as dependency via sbt build
        .option(
          "table",
          "cf-data-analytics.raw_zone.googl_data"
        )
        .load()

    val df2 = df.groupBy("dt").agg(max("trade_price")).orderBy("dt")
    // df.withColumn("test", to_date(col("dt"), "MM-dd-yyyy")).show()
    df2.show()
    print("end")

    // val df =
    //   spark.read
    //     .format("bigquery") // added as dependency via sbt build
    //     .option(
    //       "table",
    //       "cf-data-analytics.market_data.googl_latest_trade_v"
    //     )
    //     .option("viewsEnabled", "true")
    //     .load()

    // val df2 = df.repartition(16).drop("trade_condition") // create 8 partitions

    // df2.show()
    // df2.printSchema()
    // df2.write.format("parquet").save("gs://analytics-data-lake/googl-data")

    // df.show()

    // df.count()

    // df.printSchema()

    // val df2 = df.repartition(16) // create 8 partitions

    df2.write
      .format("bigquery")
      .option(
        "temporaryGcsBucket",
        "cf-spark-temp"
      ) // indirect mode destination gcs bucket
      .option("writeMethod", "direct")
      .mode("overwrite") // overwrite or append to destination table
      .save(
        "cf-data-analytics.composer_destination.googl_spark_summarized"
      ) // define destination table
  }
}
