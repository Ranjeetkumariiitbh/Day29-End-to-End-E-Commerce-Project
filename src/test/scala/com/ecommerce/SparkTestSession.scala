package com.ecommerce

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Mixin that gives a suite a small local SparkSession and stops it afterwards. */
trait SparkTestSession extends BeforeAndAfterAll { self: Suite =>

  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName(getClass.getSimpleName)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
