package com.andy.flink.wc

import org.apache.flink.api.java.ExecutionEnvironment
/**
  * <p>
  *
  * @author leone
  * @since 2019-02-28
  **/
object FlinkWordCount {

    def main(args: Array[String]): Unit = {
      // 设置运行环境
      val env = ExecutionEnvironment.getExecutionEnvironment

      // 创造测试数据
      val words = env.fromElements(
        "To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,")
    }






}
