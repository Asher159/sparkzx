package org.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCountKafka {

  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming")

    //创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //kafka参数声明
    val brokers = "node1:2181,node2:2181,node3:2181,node4:2181"
    val topic = "first"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //定义Kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    //创建KafkaCluster（维护offset）
    val kafkaCluster = new KafkaCluster(kafkaPara)

    //获取ZK中保存的offset
    val fromOffset: Map[TopicAndPartition, Long] = getOffsetFromZookeeper(kafkaCluster, group, Set(topic))

    //读取kafka数据创建DStream
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc,
      kafkaPara,
      fromOffset,
      (x: MessageAndMetadata[String, String]) => x.message())

    //数据处理
    kafkaDStream.print

    //提交offset
    offsetToZookeeper(kafkaDStream, kafkaCluster, group)

    ssc.start()
    ssc.awaitTermination()
  }

  //从ZK获取offset
  def getOffsetFromZookeeper(kafkaCluster: KafkaCluster, kafkaGroup: String, kafkaTopicSet: Set[String]): Map[TopicAndPartition, Long] = {

    // 创建Map存储Topic和分区对应的offset
    val topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]()

    // 获取传入的Topic的所有分区
    // Either[Err, Set[TopicAndPartition]]  : Left(Err)   Right[Set[TopicAndPartition]]
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(kafkaTopicSet)

    // 如果成功获取到Topic所有分区
    // topicAndPartitions: Set[TopicAndPartition]
    if (topicAndPartitions.isRight) {
      // 获取分区数据
      // partitions: Set[TopicAndPartition]
      val partitions: Set[TopicAndPartition] = topicAndPartitions.right.get

      // 获取指定分区的offset
      // offsetInfo: Either[Err, Map[TopicAndPartition, Long]]
      // Left[Err]  Right[Map[TopicAndPartition, Long]]
      val offsetInfo: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(kafkaGroup, partitions)

      if (offsetInfo.isLeft) {

        // 如果没有offset信息则存储0
        // partitions: Set[TopicAndPartition]
        for (top <- partitions)
          topicPartitionOffsetMap += (top -> 0L)
      } else {

        // 如果有offset信息则存储offset
        // offsets: Map[TopicAndPartition, Long]
        val offsets: Map[TopicAndPartition, Long] = offsetInfo.right.get
        for ((top, offset) <- offsets)
          topicPartitionOffsetMap += (top -> offset)
      }
    }
    topicPartitionOffsetMap.toMap
  }

  //提交offset
  def offsetToZookeeper(kafkaDstream: InputDStream[String], kafkaCluster: KafkaCluster, kafka_group: String): Unit = {
    kafkaDstream.foreachRDD {
      rdd =>
        // 获取DStream中的offset信息
        // offsetsList: Array[OffsetRange]
        // OffsetRange: topic partition fromoffset untiloffset
        val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 遍历每一个offset信息，并更新Zookeeper中的元数据
        // OffsetRange: topic partition fromoffset untiloffset
        for (offsets <- offsetsList) {
          val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
          // ack: Either[Err, Map[TopicAndPartition, Short]]
          // Left[Err]
          // Right[Map[TopicAndPartition, Short]]
          val ack: Either[Err, Map[TopicAndPartition, Short]] = kafkaCluster.setConsumerOffsets(kafka_group, Map((topicAndPartition, offsets.untilOffset)))
          if (ack.isLeft) {
            println(s"Error updating the offset to Kafka cluster: ${ack.left.get}")
          } else {
            println(s"update the offset to Kafka cluster: ${offsets.untilOffset} successfully")
          }
        }
    }
  }
}