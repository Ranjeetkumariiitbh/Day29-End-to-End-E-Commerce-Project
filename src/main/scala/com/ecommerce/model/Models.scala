package com.ecommerce.model

import java.sql.Timestamp

// NOTE: field names are snake_case on purpose. Dataset[T] encoders map case-class
// fields to DataFrame columns BY NAME, so they must match the column names exactly.

/** A validated order (output of the CLEAN layer). One line item per order. */
final case class Order(
    order_id: String,
    customer_id: String,
    product_id: String,
    quantity: Int,
    unit_price: Double,
    order_ts: Timestamp,
    status: String,
    payment_method: String
)

/** An order with its computed line amount (typed Dataset transformation in ENRICH). */
final case class OrderLine(
    order_id: String,
    customer_id: String,
    product_id: String,
    quantity: Int,
    unit_price: Double,
    order_ts: Timestamp,
    status: String,
    payment_method: String,
    line_amount: Double
)

/** One parsed clickstream event (RDD component). */
final case class ClickEvent(
    ts: String,
    customerId: String,
    productId: String,
    eventType: String,
    sessionId: String
)

/** Row counts reported at the end of a pipeline run (also used by tests). */
final case class PipelineSummary(
    rawOrders: Long,
    cleanOrders: Long,
    rejectedOrders: Long,
    enrichedOrders: Long,
    productsInFunnel: Long,
    rejectedClickLines: Long
) {
  def pretty: String =
    s"""
       |================ PIPELINE SUMMARY ================
       |raw orders ingested        : $rawOrders
       |clean orders (valid)       : $cleanOrders
       |rejected orders            : $rejectedOrders
       |enriched orders            : $enrichedOrders
       |products in click funnel   : $productsInFunnel
       |rejected clickstream lines : $rejectedClickLines
       |==================================================""".stripMargin
}
