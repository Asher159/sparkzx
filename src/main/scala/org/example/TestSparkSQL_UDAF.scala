package org.example

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object TestSparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()

    val ageAvgUDAF = new AgeAvgUDAF
    spark.udf.register("ageAvg", ageAvgUDAF)


    val dataframe = spark.read.json("E:\\IDEA\\projectPath\\sparkzx\\src\\main\\resources\\user.json")
//    val ds = dataframe.as[Person]

    dataframe.createOrReplaceTempView("people")

    spark.sql("select ageAvg(age) from people").show()

    spark.stop()
  }

}


/**
 * 自定义年龄平均值的聚合函数 : 弱类型
 * 1.继承 UserDefinedAggregateFunction
 * 2.重写方法
 */
class AgeAvgUDAF extends UserDefinedAggregateFunction {

  // 聚合函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //聚合函数处理逻辑（缓存）的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //聚合函数返回的结果的数据结构
  override def dataType: DataType = DoubleType

  //  聚合函数的稳定性
  override def deterministic: Boolean = true

  //  聚合函数的缓存的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //  聚合函数的同一节点的更新操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //  聚合函数的不同节点的合并操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 聚合函数的计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
