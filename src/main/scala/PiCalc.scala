import scala.math.random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

object BqDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Spark Pi")
      .config("spark.master", "local")
      .getOrCreate()

    val df =
      (spark.read
        .format("bigquery")
        .option("table", "bigquery-public-data.stackoverflow.users")
        .load()
        .cache())

    df.show(30)

    print("total row count: " + df.count())
  }
  println("Test Result Number 15")

}
