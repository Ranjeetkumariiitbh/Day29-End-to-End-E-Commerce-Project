package com.ecommerce.raw

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, input_file_name}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * RAW layer ("bronze"): ingest source files exactly as they arrive.
 *
 * Orders are read with an ALL-STRING schema so that dirty values ("abc" as a price, a bad
 * date, ...) never make the read fail - validation and typing are the CLEAN layer's job.
 * Two lineage columns are added (_source_file, _ingested_at).
 */
object RawIngestion {

  val OrderColumns: Seq[String] = Seq(
    "order_id", "customer_id", "product_id", "quantity",
    "unit_price", "order_ts", "status", "payment_method"
  )

  private val rawOrderSchema: StructType =
    StructType(OrderColumns.map(name => StructField(name, StringType, nullable = true)))

  def readOrders(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(rawOrderSchema) // explicit schema: no extra pass over the data for inference
      .csv(path)
      .withColumn("_source_file", input_file_name())
      .withColumn("_ingested_at", current_timestamp())

  /** Reference data is trusted and small, so it gets a proper typed schema. */
  def readCustomers(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .schema("customer_id STRING, name STRING, email STRING, country STRING, signup_date DATE")
      .csv(path)

  def readProducts(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .schema("product_id STRING, product_name STRING, category STRING, cost_price DOUBLE")
      .csv(path)
}
