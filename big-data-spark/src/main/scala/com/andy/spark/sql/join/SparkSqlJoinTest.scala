package com.andy.spark.sql.join

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-08
  **/
object SparkSqlJoinTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sparkJoin").master("local[*]").getOrCreate()

    import spark.implicits._

    val df1 = Seq((0, "playing"), (1, "playing"), (2, "playing")).toDF("id", "token")

    val df2 = Seq((0, "P"), (1, "W"), (2, "S")).toDF("aid", "aToken")

    // df1.cache().count()

    val result = df1.join(df2, $"id" === $"aid")

    // 查看执行计划
    result.explain()

    // 显示
    result.show()

    spark.stop()

  }

}
