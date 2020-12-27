package cn.qphone.flink.day2

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Demo6_Transformation_Filter {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val socketStream: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)
        val wcStream: DataStream[WordCount] = socketStream.flatMap(_.split("\\s+")).filter(_.length > 4).map(WordCount(_, 1))
          .keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1))
          .sum("cnt")
        wcStream.print().setParallelism(1)
        env.execute(this.getClass.getSimpleName)
    }
}
case class WordCount(word:String,cnt:Int)