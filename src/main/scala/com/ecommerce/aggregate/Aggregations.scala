package com.ecommerce.aggregate

import com.ecommerce.udf.Udfs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** AGGREGATE layer ("gold"): business-ready tables built from the persisted enriched data. */
object Aggregations {

  private def revenueOnly(df: DataFrame): DataFrame = df.filter(col("is_revenue"))

  /** groupBy + multiple aggregates. WIDE: partial aggregate -> Exchange -> final aggregate. */
  def dailyRevenueByCategory(enriched: DataFrame): DataFrame =
    revenueOnly(enriched)
      .groupBy("order_date", "category")
      .agg(
        round(sum("line_amount"), 2).as("revenue"),
        round(sum("profit"), 2).as("profit"),
        countDistinct("order_id").as("orders"),
        sum("quantity").as("units")
      )
      .orderBy("order_date", "category")

  /**
   * Running total, 7-day moving average and day-over-day change via window frames.
   * The window has no partitionBy, so Spark moves all (tiny) daily rows to ONE partition -
   * fine for one row per day, a bottleneck for large data (then partition by e.g. region).
   */
  def dailyRevenueTrend(enriched: DataFrame): DataFrame = {
    val daily = revenueOnly(enriched)
      .groupBy("order_date")
      .agg(round(sum("line_amount"), 2).as("revenue"), countDistinct("order_id").as("orders"))

    val byDate = Window.orderBy("order_date")
    daily
      .withColumn("cumulative_revenue",
        round(sum("revenue").over(byDate.rowsBetween(Window.unboundedPreceding, Window.currentRow)), 2))
      .withColumn("revenue_7d_avg", round(avg("revenue").over(byDate.rowsBetween(-6, 0)), 2))
      .withColumn("day_over_day_change", round(col("revenue") - lag("revenue", 1).over(byDate), 2))
      .orderBy("order_date")
  }

  /** Per-customer order timeline: running spend (sum window) and gap since previous order (lag). */
  def customerTimeline(enriched: DataFrame): DataFrame = {
    val perCustomer = Window.partitionBy("customer_id").orderBy("order_ts")
    revenueOnly(enriched)
      .withColumn("order_number", row_number().over(perCustomer))
      .withColumn("running_spend",
        round(sum("line_amount").over(perCustomer.rowsBetween(Window.unboundedPreceding, Window.currentRow)), 2))
      .withColumn("prev_order_ts", lag("order_ts", 1).over(perCustomer))
      .withColumn("days_since_prev_order", datediff(col("order_ts"), col("prev_order_ts")))
      .select("customer_id", "order_id", "order_number", "order_ts", "line_amount",
        "running_spend", "prev_order_ts", "days_since_prev_order")
      .orderBy("customer_id", "order_ts")
  }

  /** Customer lifetime value + tier (UDF) + dense_rank of customers inside their country. */
  def customerSummary(enriched: DataFrame): DataFrame = {
    val byCountry = Window.partitionBy("country").orderBy(col("total_spend").desc, col("customer_id"))
    revenueOnly(enriched)
      .groupBy("customer_id", "customer_name", "country")
      .agg(
        round(sum("line_amount"), 2).as("total_spend"),
        countDistinct("order_id").as("orders"),
        round(avg("line_amount"), 2).as("avg_order_value"), // one line item per order => AOV
        min("order_ts").as("first_order_ts"),
        max("order_ts").as("last_order_ts")
      )
      .withColumn("tier", Udfs.customerTierUdf(col("total_spend")))
      .withColumn("rank_in_country", dense_rank().over(byCountry))
      .orderBy(col("total_spend").desc)
  }

  /** Top-N products per category by revenue (row_number window), with funnel metrics attached. */
  def topProductsPerCategory(enriched: DataFrame, n: Int = 3): DataFrame = {
    val byCategory = Window.partitionBy("category").orderBy(col("revenue").desc, col("product_id"))
    revenueOnly(enriched)
      .groupBy("category", "product_id", "product_name")
      .agg(
        round(sum("line_amount"), 2).as("revenue"),
        sum("quantity").as("units"),
        round(sum("profit"), 2).as("profit"),
        first("views").as("views"),                 // funnel columns are constant per product
        first("purchases").as("purchases"),
        first("view_to_purchase_rate").as("view_to_purchase_rate")
      )
      .withColumn("rank_in_category", row_number().over(byCategory))
      .filter(col("rank_in_category") <= n)
      .orderBy("category", "rank_in_category")
  }

  /** Payment-method mix; the grand total is a 1-row DataFrame, broadcast into a crossJoin. */
  def paymentMix(enriched: DataFrame): DataFrame = {
    val perMethod = revenueOnly(enriched)
      .groupBy("payment_method")
      .agg(round(sum("line_amount"), 2).as("revenue"), countDistinct("order_id").as("orders"))
    val grand = perMethod.agg(sum("revenue").as("grand_total"))
    perMethod
      .crossJoin(broadcast(grand))
      .withColumn("revenue_share_pct", round(col("revenue") / col("grand_total") * 100, 2))
      .drop("grand_total")
      .orderBy(col("revenue").desc)
  }
}
