package org.example

import org.apache.spark.sql.SparkSession

object SparkSQL_Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
    import spark.implicits._
    val tbStockRdd = spark.sparkContext.textFile("E:\\IDEA\\projectPath\\sparkzx\\src\\main\\resources\\tbStock.txt")
    val tbStockDS = tbStockRdd.map(_.split("\t")).map(attr=>tbStock(attr(0),attr(1),attr(2))).toDS
    val tbStockDetailRdd = spark.sparkContext.textFile("E:\\IDEA\\projectPath\\sparkzx\\src\\main\\resources\\tbStockDetail.txt")
    val tbStockDetailDS = tbStockDetailRdd.map(_.split("\t")).map(attr=> tbStockDetail(attr(0),attr(1).trim().toInt,attr(2),attr(3).trim().toInt,attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS
    val tbDateRdd = spark.sparkContext.textFile("E:\\IDEA\\projectPath\\sparkzx\\src\\main\\resources\\tbDate.txt")
    val tbDateDS = tbDateRdd.map(_.split("\t")).map(attr=> tbDate(attr(0),attr(1).trim(), attr(2).trim().toInt,attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDS

    tbStockDS.createOrReplaceTempView("tbStock")
    tbDateDS.createOrReplaceTempView("tbDate")
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

    spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear").show
    spark.sql("SELECT a.dateid, a.ordernumber, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber GROUP BY a.dateid, a.ordernumber").show
    spark.stop()
  }

}
case class tbStock(ordernumber:String,locationid:String,dateid:String) extends Serializable
case class tbStockDetail(ordernumber:String, rownum:Int, itemid:String, number:Int, price:Double, amount:Double) extends Serializable
case class tbDate(dateid:String, years:String, theyear:Int, month:Int, day:Int, weekday:Int, week:Int, quarter:Int, period:Int, halfmonth:Int) extends Serializable