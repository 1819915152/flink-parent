package cn.qphone.flink.day6

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object Demo2_Window {
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
          //.timeWindow(Time.seconds(5))  //滚动窗口，只能统计当前窗口数据
          //.timeWindow(Time.seconds(5),Time.seconds(5))   //滑动窗口
          //.countWindow(3) //滚动
          .countWindow(5,2) //滑动
          .sum(1).print()

        env.execute()
    }
}
