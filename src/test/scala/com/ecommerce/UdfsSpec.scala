package com.ecommerce

import com.ecommerce.udf.Udfs
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class UdfsSpec extends AnyFunSuite with Matchers {

  test("orderSize buckets amounts at the 100 / 500 boundaries") {
    Udfs.orderSize(99.99) shouldBe "SMALL"
    Udfs.orderSize(100.0) shouldBe "MEDIUM"
    Udfs.orderSize(499.99) shouldBe "MEDIUM"
    Udfs.orderSize(500.0) shouldBe "LARGE"
  }

  test("customerTier uses 300 / 1000 thresholds") {
    Udfs.customerTier(0.0) shouldBe "BRONZE"
    Udfs.customerTier(300.0) shouldBe "SILVER"
    Udfs.customerTier(999.99) shouldBe "SILVER"
    Udfs.customerTier(1000.0) shouldBe "GOLD"
  }

  test("maskEmail hides the local part and is null/garbage safe") {
    Udfs.maskEmail("asha.rao@example.com") shouldBe "a***@example.com"
    Udfs.maskEmail("no-at-sign") shouldBe "***"
    Udfs.maskEmail("@example.com") shouldBe "***"
    Udfs.maskEmail(null) shouldBe null
  }
}
