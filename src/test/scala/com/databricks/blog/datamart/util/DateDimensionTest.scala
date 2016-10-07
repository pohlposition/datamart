package com.databricks.blog.datamart.util

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class DateDimensionTest extends FunSuite with SharedSparkContext {

  test("test DateDimension default does NOT generate duplicate date_value") {
    //There should never be any duplicate date_values
    val dateDimDefault = new DateDimension().create()

    val duplicateDates = dateDimDefault.groupBy("date_value")
      .agg(count("date_value"))
      .where("count(date_value) > 1")

    assert(duplicateDates.count === 0)
  }
}
