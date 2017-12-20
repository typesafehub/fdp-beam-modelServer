package kafka.client

import com.lightbend.configuration.ApplicationKafkaParameters._
import kafka.MessageListener

object DataReader {

  def main(args: Array[String]) {

    println(s"Using kafka brokers at ${LOCAL_KAFKA_BROKER}")

    val listener = MessageListener(LOCAL_KAFKA_BROKER, DATA_TOPIC, DATA_GROUP, new RecordProcessor())
    listener.start()
  }
}