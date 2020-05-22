package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
//    val sparkConf = new SparkConf().setAppName("Mock").setMaster("local[*]")
//    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
//    val conf = new SparkConf().setMaster("local[*]")

    val sc = new SparkContext(conf)
    val line = sc.textFile("C:\\Users\\foxconn\\Desktop\\SparkStudy\\agent.log")
    val provinceAdAndOne = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }
    val provinceAdToSum = provinceAdAndOne.reduceByKey(_ + _)
    val provinceToAdSum = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2)))
    val provinceGroup = provinceToAdSum.groupByKey()
    val provinceAdTop3 = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }
    provinceAdTop3.collect().foreach(println)
    sc.stop()





  }

}
