package com.ecommerce.clean

import com.ecommerce.model.Order
import com.ecommerce.raw.RawIngestion.OrderColumns
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * CLEAN layer ("silver"): type, validate and de-duplicate raw orders.
 *
 * Nothing is silently dropped: invalid and duplicate rows go to a `rejected` DataFrame with a
 * `reject_reason` and the original raw record (JSON) so they can be audited or replayed.
 */
object OrderCleaner {

  val ValidStatuses: Seq[String] = Seq("COMPLETED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED")

  final case class CleanResult(valid: Dataset[Order], rejected: DataFrame)

  /** Blank / whitespace-only strings become NULL; everything else is trimmed. */
  private def blankToNull(c: Column): Column = when(length(trim(c)) > 0, trim(c))

  def clean(raw: DataFrame): CleanResult = {
    val spark = raw.sparkSession
    import spark.implicits._

    // Keep the untouched raw values (as JSON) for the rejected-records audit trail.
    val rawRecord = to_json(struct(OrderColumns.map(c => col(c)): _*))

    // --- 1. typing + normalisation (narrow transformation: select) -------------------------
    val staged = raw.select(
      rawRecord.as("raw_record"),
      blankToNull(col("order_id")).as("order_id"),
      blankToNull(col("customer_id")).as("customer_id"),
      blankToNull(col("product_id")).as("product_id"),
      blankToNull(col("quantity")).cast("int").as("quantity"),         // "abc" -> NULL
      blankToNull(col("unit_price")).cast("double").as("unit_price"),  // "abc" -> NULL
      to_timestamp(blankToNull(col("order_ts")), "yyyy-MM-dd HH:mm:ss").as("order_ts"),
      upper(blankToNull(col("status"))).as("status"),
      coalesce(upper(blankToNull(col("payment_method"))), lit("UNKNOWN")).as("payment_method")
    )

    // --- 2. validation rules: first matching rule wins ------------------------------------
    val flagged = staged.withColumn(
      "reject_reason",
      when(col("order_id").isNull, "MISSING_ORDER_ID")
        .when(col("customer_id").isNull, "MISSING_CUSTOMER_ID")
        .when(col("product_id").isNull, "MISSING_PRODUCT_ID")
        .when(col("quantity").isNull || col("quantity") <= 0, "INVALID_QUANTITY")
        .when(col("unit_price").isNull || col("unit_price") <= 0, "INVALID_PRICE")
        .when(col("order_ts").isNull, "INVALID_TIMESTAMP")
        .when(col("status").isNull || !col("status").isin(ValidStatuses: _*), "INVALID_STATUS")
    )

    val invalid    = flagged.filter(col("reject_reason").isNotNull)
    val candidates = flagged.filter(col("reject_reason").isNull)

    // --- 3. de-duplication: keep the LATEST record per order_id ----------------------------
    // Window function => WIDE: Exchange hashpartitioning(order_id) + Sort within partitions.
    val latestFirst = Window.partitionBy("order_id").orderBy(col("order_ts").desc, col("raw_record").desc)
    val ranked      = candidates.withColumn("rn", row_number().over(latestFirst))

    val valid = ranked
      .filter(col("rn") === 1)
      .select(OrderColumns.map(c => col(c)): _*)
      .as[Order] // DataFrame -> typed Dataset[Order]; fails at analysis time if columns/types mismatch

    val duplicates = ranked
      .filter(col("rn") > 1)
      .drop("rn")
      .withColumn("reject_reason", lit("DUPLICATE_ORDER_ID"))

    CleanResult(valid, invalid.unionByName(duplicates))
  }
}
