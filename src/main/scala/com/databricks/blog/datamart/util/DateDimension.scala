package com.databricks.blog.datamart.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class DateDimension (startDate: String = "1970-01-01",
                     dateFormatMask : String = "yyyy-MM-dd",
                     numYears : Int  = 100,
                     spark: SparkSession = SparkSession.builder().getOrCreate()) extends Serializable  {

  private val dtFormat = DateTimeFormatter.ISO_LOCAL_DATE
  private val startTime = LocalDate.parse(startDate, dtFormat)
  private val startTimeEpoch = startTime.toEpochDay
  private var endTime = startTime.plusYears(numYears)

  def createDataFrame() = {
    //Get an exact number of Days based on number of calendar years after start date
    val numberOfDaysToGenerate = DAYS.between(startTime, endTime)

    spark.range(numberOfDaysToGenerate)
      .withColumnRenamed("id","date_seq")
      .selectExpr("*", s"CASE date_seq WHEN 0 THEN $startTimeEpoch ELSE $startTimeEpoch + (date_seq * 86400) END as unix_time")
      .selectExpr("*", "to_date(from_unixtime(unix_time)) as date_value")
      .withColumn("date_key", from_unixtime(col("unix_time"), "yyyyMMdd").cast("Int"))
      .withColumn("year_key", year(from_unixtime(col("unix_time"))))
      .withColumn("holiday", lit(false))
      .withColumn("year", year(from_unixtime(col("unix_time"))))
      .withColumn("quarter_of_year", quarter(from_unixtime(col("unix_time"))))
      .withColumn("month_of_year", month(from_unixtime(col("unix_time"))))
      .withColumn("day_number_of_week", from_unixtime(col("unix_time"), "u").cast("Int"))
      .selectExpr("*", """CASE WHEN day_number_of_week > 5 THEN true ELSE false END as weekend""")
      .withColumn("day_of_week_short", from_unixtime(col("unix_time"), "EEE"))
      .withColumn("day_of_week_long", from_unixtime(col("unix_time"), "EEEEEEEEE"))
      .withColumn("month_short", from_unixtime(col("unix_time"), "MMM"))
      .withColumn("month_long", from_unixtime(col("unix_time"), "MMMMMMMM"))
      .withColumn("week_key",expr("date_format(date_value, 'YYYYww')"))
      .selectExpr("*", """CASE WHEN month_of_year < 10
        THEN cast(concat(year,'0', month_of_year) as Int)
        ELSE cast(concat(year, month_of_year) as Int)
        END as month_key""")
      .selectExpr("*", s"cast(concat(year, quarter_of_year) as Int) as quarter_key")
      .selectExpr("*", s"concat('Q', quarter_of_year) as quarter_short")
  }

}
