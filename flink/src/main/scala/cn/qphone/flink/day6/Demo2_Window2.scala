package cn.qphone.flink.day6

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object Demo2_Window2 {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._
        val socket: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)

        /**
         * 20201111 chongqing 3
         * 20201112 hangzhou 2
         */

        socket.map(line=>{
            val fields: Array[String] = line.split("\\s+")
            val date: String = fields(0).trim
            val province: String = fields(1)
            val add: Int = fields(2).toInt
            (date+"_"+province,add)
        }).keyBy(0)
          .timeWindow(Time.seconds(1)) //滚动窗口，只统计当前窗口数据
          .aggregate(new AggregateFunction[(String,Int),(String,Int,Int),(String,Int)]{
              /**
               * 初始化
               * @return
               */
              override def createAccumulator(): (String, Int, Int) = ("",0,0)

              /**
               * 每读取一条记录，累加一次
               * @param value
               * @param accumulator
               * @return
               */
              override def add(value: (String, Int), accumulator: (String, Int, Int)): (String, Int, Int) = {
                  val cnt: Int = accumulator._2+1
                  val sum: Int = accumulator._3+value._2
                  (value._1,cnt,sum)
              }

              /**
               * 获取结果
               * @param accumulator
               * @return
               */
              override def getResult(accumulator: (String, Int, Int)): (String, Int) = {
                  (accumulator._1,accumulator._3/accumulator._2)
              }

              /**
               * 多个分区结果合并
               * @param partition1
               * @param partition2
               * @return
               */
              override def merge(partition1: (String, Int, Int), partition2: (String, Int, Int)): (String, Int, Int) = {
                  val cnt: Int = partition1._2+partition2._2
                  val sum: Int = partition1._3+partition2._3
                  (partition1._1,cnt,sum)
              }
          }).print()

        env.execute()

    }
}
