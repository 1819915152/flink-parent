package cn.qphone.flink.day2

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * union:DataStream*->DataStream
 * 作用：和Spark Sql中的union相似
 *
 * connect :*-->ConnectedStream
 * 作用:将两个流进行连接，两个流的类型可以不同，两个流会共享状态。
 */
object Demo8_Transformation_Union_Connect {
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
        val oldStream: DataStream[User] = splitStream.select("old")
        val newStream: DataStream[User] = splitStream.select("new")

        val unionStream: DataStream[User] = oldStream.union(newStream)
        unionStream.print("union 合并结果 :").setParallelism(1)

        env.execute("union")

    }
}
