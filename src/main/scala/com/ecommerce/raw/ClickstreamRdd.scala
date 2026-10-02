package com.ecommerce.raw

import com.ecommerce.model.ClickEvent
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator

/**
 * RDD / Pair-RDD component: parse semi-structured clickstream log lines and build a
 * per-product conversion funnel.
 *
 * Why an RDD here? The input is a pipe-delimited text log with malformed lines, and the
 * parsing is arbitrary Scala code. Pair-RDD APIs (reduceByKey + an explicit partitioner)
 * also let us show partition control directly.
 *
 * Line format:  timestamp|customer_id|product_id|event|session_id
 * where event is one of VIEW, ADD_TO_CART, PURCHASE.
 */
object ClickstreamRdd {

  val ValidEvents: Set[String] = Set("VIEW", "ADD_TO_CART", "PURCHASE")

  /** Pure parser (no Spark): returns None for malformed lines. */
  def parseLine(line: String): Option[ClickEvent] = {
    val parts = line.split("\\|", -1).map(_.trim) // -1 keeps trailing empty fields
    if (parts.length != 5 || parts.exists(_.isEmpty)) None
    else {
      val event = parts(3).toUpperCase
      if (ValidEvents.contains(event)) Some(ClickEvent(parts(0), parts(1), parts(2), event, parts(4)))
      else None
    }
  }

  /** The funnel RDD plus the accumulator counting rejected lines (read it AFTER an action). */
  final case class FunnelResult(
      funnel: RDD[(String, (Long, Long, Long))], // productId -> (views, carts, purchases)
      rejectedLines: LongAccumulator
  )

  def productFunnel(
      sc: SparkContext,
      path: String,
      validProducts: Set[String],
      numPartitions: Int
  ): FunnelResult = {

    // Accumulator: write-only counter aggregated on the driver. Updated inside a transformation,
    // so the value can over-count if a task is retried - fine for monitoring, never for logic.
    val rejected = sc.longAccumulator("rejectedClickLines")

    // Broadcast variable: ship the product-id set to each executor ONCE (not once per task).
    val productsBc = sc.broadcast(validProducts)

    val events: RDD[ClickEvent] = sc
      .textFile(path, numPartitions) // narrow: one partition per input split
      .flatMap { line =>             // narrow transformation (no shuffle)
        parseLine(line) match {
          case Some(e) if productsBc.value.contains(e.productId) => Some(e)
          case _                                                 => rejected.add(1L); None
        }
      }

    val pairs: RDD[(String, (Long, Long, Long))] = events.map { e => // Pair RDD: (key, value)
      e.eventType match {
        case "VIEW"        => (e.productId, (1L, 0L, 0L))
        case "ADD_TO_CART" => (e.productId, (0L, 1L, 0L))
        case _             => (e.productId, (0L, 0L, 1L))
      }
    }

    // WIDE transformation => the one shuffle of this component. Passing the partitioner here
    // gives: map-side combine (partial sums before the shuffle) + a known output partitioning.
    // (partitionBy(...).reduceByKey(...) would shuffle every raw record; groupByKey even worse.)
    val funnel = pairs.reduceByKey(
      new HashPartitioner(numPartitions),
      (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
    )

    FunnelResult(funnel, rejected)
  }

  /** Bridge RDD -> DataFrame so the funnel can be joined with orders in the ENRICH layer. */
  def toDataFrame(spark: SparkSession, funnel: RDD[(String, (Long, Long, Long))]): DataFrame = {
    import spark.implicits._
    funnel
      .map { case (productId, (views, carts, purchases)) => (productId, views, carts, purchases) }
      .toDF("product_id", "views", "carts", "purchases")
  }
}
