package cn.qphone.flink.day2

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}


object Demo5_DataStreamCustomSource_Kafka {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val properties = new Properties()

        properties.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
        val topic = "flink"
        val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties))

        kafkaDataStream.print("kafka source--->").setParallelism(1)

        env.execute("kafka source")
    }
}
