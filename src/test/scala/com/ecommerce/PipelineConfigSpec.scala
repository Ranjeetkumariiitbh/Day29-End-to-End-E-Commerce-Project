package com.ecommerce

import com.ecommerce.config.PipelineConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PipelineConfigSpec extends AnyFunSuite with Matchers {

  test("defaults are used when no arguments are given") {
    PipelineConfig.parse(Array.empty[String]) shouldBe PipelineConfig()
  }

  test("key/value arguments override defaults") {
    val cfg = PipelineConfig.parse(
      Array("--input", "in", "--output", "out", "--shuffle-partitions", "3", "--explain", "false", "--show", "false"))
    cfg.inputDir shouldBe "in"
    cfg.outputDir shouldBe "out"
    cfg.shufflePartitions shouldBe 3
    cfg.explain shouldBe false
    cfg.show shouldBe false
  }
}
