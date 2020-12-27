package cn.qphone.flink.day2

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

object Demo1_DataStreamSource {
    def main(args: Array[String]): Unit = {
        //1.获取上下文
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //2.source

        //2.1 list source
        val list: ListBuffer[Int] = ListBuffer(10, 20, 30)

        import org.apache.flink.api.scala._

        val dStream: DataStream[Int] = env.fromCollection(list)
        val mapStream1: DataStream[Int] = dStream.map(_ * 100)
        mapStream1.print().setParallelism(1)
        println("====================================")

        //2.2 string source
        val dStream2: DataStream[String] = env.fromElements("wangjunjie hen shuai")
        dStream2.print().setParallelism(1)
        println("====================================")

        //2.3 文件作为源
        val dStream3: DataStream[String] = env.readTextFile("file:///E:/data/hello.txt", "utf-8")
        dStream3.print().setParallelism(1)
        println("====================================")

        //2.4 socket作为源
        val dStream4: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)
        dStream4.print().setParallelism(1)
        println("====================================")


        env.execute("collection source")
    }
}
