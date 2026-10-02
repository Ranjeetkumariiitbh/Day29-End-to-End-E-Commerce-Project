package com.ecommerce.config

/** Runtime configuration, parsed from `--key value` command-line pairs. */
final case class PipelineConfig(
    inputDir: String = "data/raw",
    outputDir: String = "output",
    master: String = "local[*]",
    shufflePartitions: Int = 8,
    explain: Boolean = true,
    show: Boolean = true
)

object PipelineConfig {

  def parse(args: Array[String]): PipelineConfig =
    args.sliding(2, 2).foldLeft(PipelineConfig()) {
      case (cfg, Array("--input", v))              => cfg.copy(inputDir = v)
      case (cfg, Array("--output", v))             => cfg.copy(outputDir = v)
      case (cfg, Array("--master", v))             => cfg.copy(master = v)
      case (cfg, Array("--shuffle-partitions", v)) => cfg.copy(shufflePartitions = v.toInt)
      case (cfg, Array("--explain", v))            => cfg.copy(explain = v.toBoolean)
      case (cfg, Array("--show", v))               => cfg.copy(show = v.toBoolean)
      case (cfg, _)                                => cfg
    }
}
