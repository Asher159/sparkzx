package org.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建Spark配置信息
    val streamingConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    val ssc= new StreamingContext(streamingConf, Seconds(5))

    //采集数据
//    val fileDStream = ssc.textFileStream("in")
    /**
     * /3.创建RDD队列
     * val rddQueue = new mutable.Queue[RDD[Int]]()
     *
     * //4.创建QueueInputDStream
     * val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)
     */
    val socketDStream = ssc.socketTextStream("192.168.1.213", 9999)

    val wordDStream = socketDStream.flatMap(_.split(" "))

    val tupleDStream = wordDStream.map(word => (word, 1))

    val resultDStream = tupleDStream.reduceByKey(_ + _)

    resultDStream.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
