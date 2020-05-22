package org.example

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.SparkSession

object TestSparkSqlUDF {
  def main(args: Array[String]): Unit = {
//    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
//    .config("spark.some.config.option", "some-value")
    val spark = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
    import spark.implicits._
    val dataframe = spark.read.json("E:\\IDEA\\projectPath\\sparkzx\\src\\main\\resources\\user.json")
    spark.udf.register("addName", (x:String)=> "Name:"+x)


//
//    dataframe.show()
    val ds= dataframe.as[Person]
    ds.createTempView("people")
    spark.sql("Select addName(name), age from people").show()


    spark.stop()


  }

}

case class Person(name:String,age:Long)
