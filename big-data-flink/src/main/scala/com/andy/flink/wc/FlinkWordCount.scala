package com.andy.flink.wc

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-28
  **/
object FlinkWordCount {

  def main(args: Array[String]): Unit = {
   /* if (args.length != 2) {
      println(s"${this.getClass.getSimpleName} must be two param:inputDir outputDir")
      System.exit(1)
    }

    // 在window环境下，以hadoop身份远程放完HDFS
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val Array(inputDir, outputDir) = args

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(inputDir)

    val result = text.flatMap(_.split("\\s"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    result.setParallelism(2).writeAsCsv(outputDir, "\n", ",", WriteMode.OVERWRITE)
    env.execute(this.getClass.getSimpleName)*/

  }


}
