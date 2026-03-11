import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HotelBookingETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hotel Booking ETL")
      .getOrCreate()

    // Step 1: Extract
    val bookingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://input-bookings.csv")

    // Step 2: Transform
    val filteredDF = bookingsDF
      .filter(col("booking_status") === "CONFIRMED")
      .filter(col("city") === "Vancouver")
      .filter(col("nights") >= 2)

    val aggregatedDF = filteredDF
      .groupBy("hotel_id")
      .agg(sum("total_price").alias("total_revenue"))

    // Step 3: Load
    aggregatedDF.write
      .option("header", "true")
      .csv("file://output-hotel_revenue_summary.csv")

    spark.stop()
  }
}