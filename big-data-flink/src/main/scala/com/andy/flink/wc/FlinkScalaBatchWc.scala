package com.andy.flink.wc

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-01
  **/
object FlinkScalaBatchWc {

  def main(args: Array[String]): Unit = {
    // get env
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("file:///e:/tmp/flink/input")

    import org.apache.flink.api.scala._

    val counts = text.flatMap(_.split(" ")
      .filter(_.nonEmpty)
      .map((_, 1)))
      .groupBy(0)
      .sum(1)
      .setParallelism(1)

    counts.writeAsCsv("file:///e:/tmp/flink/output1", "\n", ",").setParallelism(1)

    env.execute("scala batch wc")

  }

}
