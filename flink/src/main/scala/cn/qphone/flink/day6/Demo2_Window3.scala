package cn.qphone.flink.day6

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Demo2_Window3 {
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
          .process[(String,Double)](new ProcessWindowFunction[(String,Int),(String,Double),Tuple,TimeWindow] {
              override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Double)]): Unit = {
                  var cnt:Int=0
                  var sum:Double=0.0
                  //遍历记录
                  elements.foreach(record=>{
                      cnt=cnt+1
                      sum=sum+record._2
                  })
                  out.collect((key.getField((0)),sum/cnt))

              }
          }).print()

        env.execute()

    }
}
