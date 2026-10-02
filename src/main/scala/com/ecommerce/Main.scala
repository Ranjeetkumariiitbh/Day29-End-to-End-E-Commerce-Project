package com.ecommerce

import com.ecommerce.common.SparkSessionProvider
import com.ecommerce.config.PipelineConfig

/** Entry point:  sbt "run --input data/raw --output output" */
object Main {
  def main(args: Array[String]): Unit = {
    val cfg   = PipelineConfig.parse(args)
    val spark = SparkSessionProvider.create("Day29-E2E-ECommerce", cfg.master, cfg.shufflePartitions)
    try println(Pipeline.run(spark, cfg).pretty)
    finally spark.stop()
  }
}
