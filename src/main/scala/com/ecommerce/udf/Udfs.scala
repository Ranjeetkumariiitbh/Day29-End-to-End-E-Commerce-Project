package com.ecommerce.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * Business-rule UDFs.
 *
 * The logic lives in plain Scala functions (unit-testable without Spark) and is then wrapped
 * with `udf(...)`. UDFs are black boxes to Catalyst: no predicate push-down, no code-gen
 * for the function body, and a serialization round-trip per row. Prefer built-in functions
 * (`when/otherwise`, `regexp_replace`, ...) when possible; use a UDF when the rule is
 * genuinely complex or shared with non-Spark code.
 */
object Udfs {

  // ---- pure functions -------------------------------------------------------------------

  def orderSize(amount: Double): String =
    if (amount >= 500.0) "LARGE" else if (amount >= 100.0) "MEDIUM" else "SMALL"

  def customerTier(totalSpend: Double): String =
    if (totalSpend >= 1000.0) "GOLD" else if (totalSpend >= 300.0) "SILVER" else "BRONZE"

  /** "asha.rao@example.com" -> "a***@example.com" (PII masking). */
  def maskEmail(email: String): String =
    if (email == null) null
    else {
      val at = email.indexOf('@')
      if (at <= 0) "***" else email.substring(0, 1) + "***" + email.substring(at)
    }

  // ---- Spark wrappers -------------------------------------------------------------------

  val orderSizeUdf: UserDefinedFunction    = udf((amount: Double) => orderSize(amount))
  val customerTierUdf: UserDefinedFunction = udf((spend: Double) => customerTier(spend))
  val maskEmailUdf: UserDefinedFunction    = udf((email: String) => maskEmail(email))

  /** Make the UDFs callable from Spark SQL: SELECT order_size(line_amount) ... */
  def register(spark: SparkSession): Unit = {
    spark.udf.register("order_size", orderSizeUdf)
    spark.udf.register("customer_tier", customerTierUdf)
    spark.udf.register("mask_email", maskEmailUdf)
  }
}
