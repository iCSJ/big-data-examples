package com.andy.flink.wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-28
  **/
object FlinkScalaWordCount {

  def main(args: Array[String]): Unit = {
    // 必须要导入隐式转换
    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("node-1", 9999, '\n')
    val result = text.flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .timeWindowAll(Time.seconds(2), Time.seconds(1))
      .sum("count")

    result.print().setParallelism(1)


    env.execute("scala—nc-wc")
  }

  case class WordWithCount(word: String, count: Long)


}
