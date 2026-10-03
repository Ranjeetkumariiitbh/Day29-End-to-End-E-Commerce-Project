package com.ecommerce

import com.ecommerce.config.PipelineConfig
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

/**
 * End-to-end test on the bundled sample data. Expected numbers were derived by hand from
 * data/raw CSV files (see README "Expected results").
 */
class PipelineIntegrationSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private lazy val outDir = Files.createTempDirectory("ecom-e2e").toString

  private lazy val summary = Pipeline.run(
    spark,
    PipelineConfig(
      inputDir = new File("data/raw").getAbsolutePath,
      outputDir = outDir,
      shufflePartitions = 4,
      explain = false,
      show = false
    )
  )

  private def gold(name: String) = spark.read.parquet(s"$outDir/04_aggregates/parquet/$name")

  test("row counts through the layers") {
    summary.rawOrders shouldBe 26L
    summary.cleanOrders shouldBe 19L
    summary.rejectedOrders shouldBe 7L
    summary.enrichedOrders shouldBe 19L
    summary.productsInFunnel shouldBe 6L
    summary.rejectedClickLines shouldBe 4L
  }

  test("total revenue (COMPLETED + SHIPPED only) and revenue order count") {
    summary // make sure the pipeline has run
    val row = spark.read.parquet(s"$outDir/03_enriched")
      .filter(col("is_revenue"))
      .agg(sum("line_amount"), countDistinct("order_id"))
      .head()
    row.getDouble(0) shouldBe (4057.48 +- 0.01)
    row.getLong(1) shouldBe 16L
  }

  test("orders with an unknown product or customer survive the left joins as UNKNOWN") {
    summary
    val enriched = spark.read.parquet(s"$outDir/03_enriched")
    enriched.filter("order_id = 'O1022'").select("category").head().getString(0) shouldBe "UNKNOWN"
    enriched.filter("order_id = 'O1016'").select("country").head().getString(0) shouldBe "UNKNOWN"
  }

  test("window: running spend and days since previous order for customer C001") {
    summary
    val rows = gold("customer_timeline").filter("customer_id = 'C001'").orderBy("order_ts").collect()
    rows.map(_.getAs[Double]("running_spend")) shouldBe Array(50.0, 170.0, 1069.0, 1131.0)
    rows.map(r => Option(r.getAs[Integer]("days_since_prev_order")).map(_.intValue)) shouldBe
      Array(None, Some(1), Some(2), Some(2))
  }

  test("window: cumulative revenue ends at total revenue") {
    summary
    val last = gold("daily_revenue_trend").agg(max("cumulative_revenue")).head().getDouble(0)
    last shouldBe (4057.48 +- 0.01)
  }

  test("UDF + dense_rank: C001 is the GOLD customer ranked #1 in IN") {
    summary
    val c1 = gold("customer_summary").filter("customer_id = 'C001'").head()
    c1.getAs[String]("tier") shouldBe "GOLD"
    c1.getAs[Double]("total_spend") shouldBe (1131.0 +- 0.01)
    c1.getAs[Int]("rank_in_country") shouldBe 1
  }
}
