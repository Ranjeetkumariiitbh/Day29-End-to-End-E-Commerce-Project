package com.ecommerce

import com.ecommerce.raw.ClickstreamRdd
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File

class ClickstreamRddSpec extends AnyFunSuite with Matchers with SparkTestSession {

  test("parseLine accepts well-formed lines and normalises the event type") {
    val e = ClickstreamRdd.parseLine("2026-09-01T09:00:01|C001|P100|view|S1")
    e.map(_.eventType) shouldBe Some("VIEW")
    e.map(_.productId) shouldBe Some("P100")
  }

  test("parseLine rejects wrong field counts, empty fields and unknown events") {
    ClickstreamRdd.parseLine("garbage line without delimiters") shouldBe None
    ClickstreamRdd.parseLine("2026-09-06T10:05:00|C002||VIEW|S11") shouldBe None
    ClickstreamRdd.parseLine("2026-09-06T10:00:00|C002|P100|CLICK|S11") shouldBe None
  }

  test("productFunnel sums events per product with a Pair RDD and counts rejected lines") {
    val path     = new File("data/raw/clickstream.log").getAbsolutePath
    val products = Set("P100", "P101", "P102", "P103", "P104", "P105")

    val result = ClickstreamRdd.productFunnel(spark.sparkContext, path, products, numPartitions = 2)
    val funnel = result.funnel.collectAsMap() // action: runs the job, fills the accumulator

    funnel("P100") shouldBe ((2L, 1L, 2L)) // views, carts, purchases
    funnel("P104") shouldBe ((4L, 2L, 2L))
    result.rejectedLines.value shouldBe 4L // garbage, CLICK event, empty product, unknown product P999
  }
}
