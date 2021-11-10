package tools

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.Source.fromFile

/**
 * Author:BYDylan
 * Date:2021/11/10
 * Description:
 */
object KafkaProducerTool {
  def main(args: Array[String]): Unit = {
    writeToKafka("hot_items")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    // 从文件读取数据，逐行写入kafka
    val bufferedSource = fromFile("D:\\WorkSpace\\ideaProject\\flink_user_behavior_analysis\\hot_items_analysis\\src\\main\\scala\\KafkaProducerUtil.scala")

    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }
}
