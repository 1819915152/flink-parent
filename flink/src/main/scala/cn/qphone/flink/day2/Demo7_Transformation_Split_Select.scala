package cn.qphone.flink.day2

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * Split：DataStream->SplitStream
 * 作用:将DataStream拆分成多个流，用splitStream
 * select:SplitStream ->DataStream
 * 作用:和Split搭配使用，从SplitStream中选择一个或者多个流组成一个新的DataStream
 *
 * 1,wdd,man,180,180
 */
object Demo7_Transformation_Split_Select {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val socketStream: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)

        val splitStream: SplitStream[User] = socketStream.map(info => {
            val arr: Array[String] = info.split(",")
            val uid: String = arr(0).trim
            val name: String = arr(1)
            val sex: String = arr(2)
            val height: Double = arr(3).toDouble
            val weight: Double = arr(4).toDouble
            User(uid, name, sex, height, weight)
        }).split((user: User) => {
            if (user.name.equals("wdd")) Seq("old")
            else Seq("new")
        })

        //判断子流被标记为大佬
        splitStream.select("old").print("wdd666").setParallelism(1)
        //判断子流被标记为马仔
        splitStream.select("new").print("所有人都是弟弟").setParallelism(1)

        env.execute("split select transformation")
    }
}
case class User(id:String,name:String,sex:String,height:Double,weight:Double)