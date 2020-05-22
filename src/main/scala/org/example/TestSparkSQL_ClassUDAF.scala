package org.example

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}


object TestSparkSQL_ClassUDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
    import spark.implicits._

    val ageAvgClassUDAF = new AgeAvgClassUDAF
    val avgAgeColumn = ageAvgClassUDAF.toColumn.name("avgAge")

    //    spark.udf.register("ageAvg", ageAvgClassUDAF)


    val dataframe = spark.read.json("E:\\IDEA\\projectPath\\sparkzx\\src\\main\\resources\\user.json")
    val ds = dataframe.as[User]

//    ds.createOrReplaceTempView("people")

    ds.select(avgAgeColumn).show()
    spark.stop()
  }

}


/**
 * 自定义年龄平均值的聚合函数 : 强类型
 * 1.继承  org.apache.spark.sql.expressions.Aggregator[输入，缓存，输出]
 * 2.重写方法
 */

case class User(name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Long)

class AgeAvgClassUDAF extends Aggregator[User, AvgBuffer, Double] {
  //初始化缓存对象
  override def zero: AvgBuffer = {
    AvgBuffer(0L, 0L)
  }

  //同一节点的缓存更新
  override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  // 不同节点的缓存更新
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //完成计算功能
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
