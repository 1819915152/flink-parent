package cn.qphone.flink.day6

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object Demo3_Trigger {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._
        val socket: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)

        /**
         * 20201111 chongqing 3
         * 20201112 hangzhou 2
         */
        socket.map(line => {
            val fields: Array[String] = line.split("\\s+")
            val date: String = fields(0).trim
            val province: String = fields(1)
            val add: Int = fields(2).toInt
            (date+"_"+province, add)
        }).keyBy(0)
          .timeWindow(Time.seconds(5)) // 滚动窗口,只统计当前窗口数据
          .trigger(new MyTrigger) // 设置触发器
          .sum(1)
          .print()

        env.execute()
    }
}

class MyTrigger extends Trigger[(String,Int), TimeWindow] {

    var cnt:Int = 0
    /**
     * 每读取一个元素，此方法会被自动调用
     */
    override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        // 注册时间触发器
        ctx.registerProcessingTimeTimer(window.maxTimestamp()) // 当前窗口的最大值
        println(window.maxTimestamp())
        if (cnt > 5) {
            println("触发的计数窗口")
            cnt = 0
            TriggerResult.FIRE
        }else {
            cnt = cnt + 1
            TriggerResult.CONTINUE
        }
    }

    /**
     * 当ProcessTime的定时器被出发的时候调用
     * @return
     */
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        println("触发的时间窗口")
        TriggerResult.FIRE
    }

    /**
     * 当eventTime的定时器被出发的时候调用
     */
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }

    /**
     * 窗口清楚的时候被调用
     */
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp())
    }
}