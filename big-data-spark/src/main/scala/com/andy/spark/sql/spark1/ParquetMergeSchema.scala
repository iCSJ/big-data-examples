package com.andy.spark.sql.spark1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-21
  **/
object ParquetMergeSchema {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("parquet").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val studentWithNameAge = Array(("leone", 15), ("jack", 23), ("andy", 34), ("james", 23)).toSeq

    val studentWithNameageDF = sc.parallelize(studentWithNameAge,2).toDF("name", "age")


  }

}
