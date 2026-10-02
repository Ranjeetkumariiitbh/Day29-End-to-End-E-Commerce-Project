package com.ecommerce

import com.ecommerce.clean.OrderCleaner
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OrderCleanerSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val cols = Seq("order_id", "customer_id", "product_id", "quantity",
    "unit_price", "order_ts", "status", "payment_method")

  private def raw(rows: (String, String, String, String, String, String, String, String)*) = {
    import spark.implicits._
    rows.toDF(cols: _*)
  }

  test("valid rows are typed and normalised (status upper-cased)") {
    val res = OrderCleaner.clean(raw(("O1", "C1", "P1", "2", "10.50", "2026-09-01 10:00:00", "completed", "card")))
    val o   = res.valid.collect().head
    o.status shouldBe "COMPLETED"
    o.payment_method shouldBe "CARD"
    o.quantity shouldBe 2
    o.unit_price shouldBe 10.5
    res.rejected.count() shouldBe 0L
  }

  test("each invalid rule produces its own reject_reason") {
    import spark.implicits._
    val ts  = "2026-09-01 10:00:00"
    val res = OrderCleaner.clean(raw(
      ("",   "C1", "P1", "1",   "10", ts,           "COMPLETED", "card"), // MISSING_ORDER_ID
      ("O2", "",   "P1", "1",   "10", ts,           "COMPLETED", "card"), // MISSING_CUSTOMER_ID
      ("O3", "C1", "P1", "0",   "10", ts,           "COMPLETED", "card"), // INVALID_QUANTITY
      ("O4", "C1", "P1", "1",   "abc", ts,          "COMPLETED", "card"), // INVALID_PRICE
      ("O5", "C1", "P1", "1",   "10", "not-a-date", "COMPLETED", "card"), // INVALID_TIMESTAMP
      ("O6", "C1", "P1", "1",   "10", ts,           "DELIVERED", "card")  // INVALID_STATUS
    ))
    res.valid.count() shouldBe 0L
    val reasons = res.rejected.select("reject_reason").as[String].collect().toSet
    reasons shouldBe Set("MISSING_ORDER_ID", "MISSING_CUSTOMER_ID", "INVALID_QUANTITY",
      "INVALID_PRICE", "INVALID_TIMESTAMP", "INVALID_STATUS")
  }

  test("duplicates keep the latest record and the older one is rejected") {
    import spark.implicits._
    val res = OrderCleaner.clean(raw(
      ("O1", "C1", "P1", "1", "10", "2026-09-01 10:00:00", "PENDING",   "card"),
      ("O1", "C1", "P1", "1", "10", "2026-09-01 10:05:00", "COMPLETED", "card")
    ))
    res.valid.collect().map(_.status) shouldBe Array("COMPLETED")
    res.rejected.select("reject_reason").as[String].collect() shouldBe Array("DUPLICATE_ORDER_ID")
  }
}
