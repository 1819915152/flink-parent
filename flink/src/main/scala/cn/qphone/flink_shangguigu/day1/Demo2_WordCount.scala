package cn.qphone.flink_shangguigu.day1

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Demo2_WordCount {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val inputStream: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)
        val resultDataStream: DataStream[(String, Int)] = inputStream.flatMap(_.split("\\s+"))
          .filter(_.nonEmpty)
          .map((_, 1))
          .keyBy(0)
          .sum(1)

        resultDataStream.print().setParallelism(4)

        env.execute()
    }
}
