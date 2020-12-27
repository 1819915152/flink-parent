package cn.qphone.flink.day5

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object Demo5_Broadcast {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._

        val desc = new MapStateDescriptor(
            "sexinfo",
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
        )

        val sex: DataStream[(Int, String)] = env.fromElements((1, "男"), (2, "女"))
        val sexB: BroadcastStream[(Int, String)] = sex.broadcast(desc)
        /**
         * lixi 1
         * wangjunjie 1
         * lihao 2
         */
        val socket: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)
        val map: DataStream[(String, Int)] = socket.map(line => {
            val fields: Array[String] = line.split("\\s+")
            val name: String = fields(0)
            val sexid: Int = fields(1).toInt
            (name, sexid)
        })

        map.connect(sexB).process(new BroadcastProcessFunction[(String, Int), (Int, String), (String, String)] {
            // 处理元素
            override def processElement(value: (String, Int),
                                        ctx: BroadcastProcessFunction[(String, Int), (Int, String), (String, String)]#ReadOnlyContext,
                                        out: Collector[(String, String)]): Unit = {
                val genderid: Int = value._2 // 获取到map数据集合中的性别编号
                var gender: String = ctx.getBroadcastState(desc).get(genderid) //从广播变量中通过编号获取到性别字符串
                if (gender == null) gender = "人妖"
                //输出
                out.collect((value._1, gender))
            }

            // 继续处理广播元素
            override def processBroadcastElement(value: (Int, String),
                                                 ctx: BroadcastProcessFunction[(String, Int), (Int, String), (String, String)]#Context,
                                                 out: Collector[(String, String)]): Unit = {
                ctx.getBroadcastState(desc).put(value._1, value._2)
            }
        }).print()
        env.execute()
    }
}
