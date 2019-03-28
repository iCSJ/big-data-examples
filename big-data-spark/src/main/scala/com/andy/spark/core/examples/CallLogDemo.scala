package com.andy.spark.core.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-18
  **/
object CallLogDemo {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("call-log").master("local[*]").getOrCreate()

    val rdd = session.sparkContext.textFile("e:/tmp/logs/web-20190218-11.txt")

    // 1550480943879	34:E4:82:E3:E1:75	15939879106	https://www.centos.org/	PC	182.83.205.68	81	3820
    val rows: RDD[Row] = rdd.map(line => {
      val fields = line.split("\t")
      val time = fields(0).toLong
      val mac = fields(1)
      val tel = fields(2)
      val url = fields(3)
      val agent = fields(4)
      val ip = fields(5)
      val up = fields(6).toLong
      val done = fields(7).toLong
      Row(time, mac, tel, url, agent, ip, up, done)
    })

    // 结果类型，表头 用户描述DataFrame
    val schema: StructType = StructType(List(
      StructField("time", LongType, true),
      StructField("mac", StringType, true),
      StructField("tel", StringType, true),
      StructField("url", StringType, true),
      StructField("agent", StringType, true),
      StructField("ip", StringType, true),
      StructField("up", LongType, true),
      StructField("done", LongType, true)
    ))

    val dataFrame: DataFrame = session.createDataFrame(rows, schema)

    import session.implicits._
    val df = dataFrame.where($"done" > 1000).orderBy($"done" desc, $"up" asc).limit(30)

    df.show()

    session.stop()
  }

}
