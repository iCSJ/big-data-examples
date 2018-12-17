package com.andy.spark.ip

import scala.io.{BufferedSource, Source}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-17
  **/
object IpUtils {

  def ipToLong(ip: String): Long = {
    val flag = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until flag.length) {
      ipNum = flag(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readRules(path: String): Array[(Long, Long, String)] = {
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }


  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2)) {
        return middle
      }
      if (ip < lines(middle)._1) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val rules: Array[(Long, Long, String)] = readRules("D:\\ip.txt")
    val ipNum = ipToLong("114.215.43.42")
    val index = binarySearch(rules, ipNum)
    val tp = rules(index)
    val province = tp._3
    println(province)
  }


}
