package com.ecommerce

import com.ecommerce.aggregate.Aggregations
import com.ecommerce.clean.OrderCleaner
import com.ecommerce.config.PipelineConfig
import com.ecommerce.enrich.Enricher
import com.ecommerce.model.{Order, PipelineSummary}
import com.ecommerce.output.Writer
import com.ecommerce.raw.{ClickstreamRdd, RawIngestion}
import com.ecommerce.udf.Udfs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

/** Orchestrates RAW -> CLEAN -> ENRICH -> AGGREGATE. Each layer is persisted to disk. */
object Pipeline {

  private def banner(title: String): Unit =
    println(s"\n${"=" * 78}\n$title\n${"=" * 78}")

  private def timed[T](label: String)(body: => T): T = {
    val t0 = System.nanoTime()
    val result = body
    println(f"[timing] $label%-32s ${(System.nanoTime() - t0) / 1e9}%.2fs")
    result
  }

  def run(spark: SparkSession, cfg: PipelineConfig): PipelineSummary = {
    import spark.implicits._

    Udfs.register(spark)

    val rawPath      = s"${cfg.outputDir}/01_raw"
    val cleanPath    = s"${cfg.outputDir}/02_clean"
    val enrichedPath = s"${cfg.outputDir}/03_enriched"
    val aggPath      = s"${cfg.outputDir}/04_aggregates"

    // ===================================================================================
    // 1. RAW  - land source data unchanged (all-string orders + lineage columns)
    // ===================================================================================
    banner("1. RAW: ingest orders as-is")
    val customers = RawIngestion.readCustomers(spark, s"${cfg.inputDir}/customers.csv")
    val products  = RawIngestion.readProducts(spark, s"${cfg.inputDir}/products.csv")
    timed("raw orders -> parquet") {
      Writer.parquet(RawIngestion.readOrders(spark, s"${cfg.inputDir}/orders.csv"), s"$rawPath/orders")
    }

    // ===================================================================================
    // 2a. CLEAN (DataFrame / Dataset) - validate, type, de-duplicate orders
    // ===================================================================================
    banner("2a. CLEAN orders (DataFrame -> Dataset[Order])")
    val rawOrders = spark.read.parquet(s"$rawPath/orders") // read back: layers talk through storage
    val cleaned   = OrderCleaner.clean(rawOrders)
    if (cfg.explain) cleaned.valid.explain("formatted") // look for ONE Exchange (window on order_id)
    timed("clean orders -> parquet") {
      Writer.parquet(cleaned.valid.toDF(), s"$cleanPath/orders")
      Writer.parquet(cleaned.rejected, s"$cleanPath/rejected_orders")
    }

    // ===================================================================================
    // 2b. CLEAN (RDD / Pair RDD) - clickstream conversion funnel
    // ===================================================================================
    banner("2b. CLEAN clickstream (RDD / Pair RDD)")
    // Action #1 (collect): small product-id set is pulled to the driver, then broadcast.
    val validProducts = products.select("product_id").as[String].collect().toSet
    val clicks = ClickstreamRdd.productFunnel(
      spark.sparkContext, s"${cfg.inputDir}/clickstream.log", validProducts, cfg.shufflePartitions)

    // MEMORY_ONLY: the funnel is tiny, and recomputing it (re-parsing text) would be wasteful.
    clicks.funnel.persist(StorageLevel.MEMORY_ONLY)
    if (cfg.explain) println("RDD lineage:\n" + clicks.funnel.toDebugString) // ShuffledRDD marks the stage boundary
    timed("click funnel -> parquet") {
      // Action #2: first action over the RDD; it materialises the cache and fills the accumulator.
      Writer.parquet(ClickstreamRdd.toDataFrame(spark, clicks.funnel), s"$cleanPath/product_funnel")
    }
    val rejectedClicks = clicks.rejectedLines.value // valid only AFTER an action has run
    clicks.funnel.unpersist()

    // ===================================================================================
    // 3. ENRICH - typed map, UDFs, broadcast joins; persisted because it is reused many times
    // ===================================================================================
    banner("3. ENRICH (broadcast joins + UDFs, persisted)")
    val cleanOrders = spark.read.parquet(s"$cleanPath/orders").as[Order]
    val funnelDf    = spark.read.parquet(s"$cleanPath/product_funnel")

    val enriched = Enricher
      .enrich(cleanOrders, customers, products, funnelDf)
      // MEMORY_AND_DISK: ~8 downstream jobs reuse this; spill to disk instead of recomputing
      // the whole lineage if it ever does not fit in memory.
      .persist(StorageLevel.MEMORY_AND_DISK)

    if (cfg.explain) enriched.explain("formatted") // look for BroadcastHashJoin, no Exchange on orders
    val enrichedCount = timed("materialise cache (count)")(enriched.count()) // action: fills the cache

    timed("enriched -> parquet (by date)") {
      // repartition(order_date) first => each date lands in ONE task => one file per date folder
      // instead of up to N small files per folder.
      Writer.parquet(enriched.repartition(col("order_date")), enrichedPath, Seq("order_date"))
    }

    // ===================================================================================
    // 4. AGGREGATE - gold tables (groupBy + windows) served from the cached enriched data
    // ===================================================================================
    banner("4. AGGREGATE (groupBy + window functions)")
    def publish(name: String, df: DataFrame): Unit = {
      Writer.parquet(df, s"$aggPath/parquet/$name")
      Writer.csvReport(df, s"$aggPath/csv/$name")
      if (cfg.show) { println(s"--- $name ---"); df.show(20, truncate = false) }
    }

    timed("aggregates") {
      publish("daily_revenue_by_category", Aggregations.dailyRevenueByCategory(enriched))
      publish("daily_revenue_trend", Aggregations.dailyRevenueTrend(enriched))
      publish("customer_summary", Aggregations.customerSummary(enriched))
      publish("top_products_per_category", Aggregations.topProductsPerCategory(enriched))
      publish("payment_mix", Aggregations.paymentMix(enriched))

      // Window partitioned by customer_id: pre-partitioning by the same key lets Spark reuse
      // that shuffle for the window instead of adding another (check explain: one Exchange).
      val byCustomer = enriched.repartition(col("customer_id"))
      val timeline   = Aggregations.customerTimeline(byCustomer)
      if (cfg.explain) timeline.explain("formatted")
      publish("customer_timeline", timeline)
    }

    // ===================================================================================
    // Data-quality summary
    // ===================================================================================
    val rejectedDf = spark.read.parquet(s"$cleanPath/rejected_orders")
    if (cfg.show) {
      banner("DATA QUALITY: rejected orders by reason")
      rejectedDf.groupBy("reject_reason").count().orderBy("reject_reason").show(false)
    }

    val summary = PipelineSummary(
      rawOrders = rawOrders.count(),
      cleanOrders = cleanOrders.count(),
      rejectedOrders = rejectedDf.count(),
      enrichedOrders = enrichedCount,
      productsInFunnel = funnelDf.count(),
      rejectedClickLines = rejectedClicks
    )

    enriched.unpersist() // release cache memory once every consumer is done
    summary
  }
}
