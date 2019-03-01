package com.andy.flink.wc

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-28
  **/
object FlinkScalaWordCount {

  def main(args: Array[String]): Unit = {
    /* val port: Int = try {
       ParameterTool.fromArgs(args).getInt("port")
     } catch {
       case e: Exception => {
         System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
         return
       }
     }*/

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("39.108.125.41", 8082, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)
    println(windowCounts)
    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
