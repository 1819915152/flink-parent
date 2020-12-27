package cn.qphone.flink.day3

import java.util.Properties

import org.apache.flink.api.common.ExecutionConfig.SerializableSerializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.common.serialization.ByteArraySerializer

object Demo2_Sink_Basic {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dStream: DataStream[String] = env.fromElements("汪俊杰", "王耀峰", "唐琦", "李熙")
        //val dStream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 33), Tuple2(100, 66), Tuple2(100, 56), Tuple2(200, 666))
        //dStream.print()  //输出到控制台
        //dStream.writeAsText("file:///E:/data/1.txt")
        //dStream.writeAsCsv("file:///E:/data/2.csv")
        //dStream.writeToSocket("192.168.10.100",9999,new SimpleStringSchema())

        //kafka
        val topic = "flink"
        val properties = new Properties()
        properties.setProperty("bootstrap.servers","192.168.10.100:9092")
        properties.setProperty("key.serializer",classOf[ByteArraySerializer].getName)
        properties.setProperty("value.serializer",classOf[ByteArraySerializer].getName)
        val sink = new FlinkKafkaProducer[String](topic, new SimpleStringSchema(), properties)

        dStream.addSink(sink)

        env.execute("Sink_Basic")

    }
}
