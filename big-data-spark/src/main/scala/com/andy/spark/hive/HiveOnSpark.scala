package com.andy.spark.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-08
  **/
object HiveOnSpark {

  def main(args: Array[String]): Unit = {

    // 创建spark如果想软hive运行在spark上需要开启hive对spark的自持enableHiveSupport()
    val spark = SparkSession.builder().master("local[*]").appName("hiveOnSpark")
      // 可以兼容hive的语法
      .enableHiveSupport()
      .getOrCreate()

    //    val result = spark.sql("select * from t_boy order by fv desc")
    //    result.show()

    val result: DataFrame = spark.sql("create table t_test(id bigint, name string)")
    result.show()
    spark.close()
  }

}
