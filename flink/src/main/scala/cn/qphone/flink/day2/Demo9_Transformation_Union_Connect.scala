package cn.qphone.flink.day2

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * union:DataStream*-->DataStream
 * 作用:和Spark sql中的union类似
 *
 * connect:*-->ConnectedStream
 * 作用:将两个流进行连接，两个流的类型可以不同，两个流会共享状态。
 */
object Demo9_Transformation_Union_Connect {
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

        val bigStream: DataStream[(String, String)] = splitStream.select("old").map(e => (e.name, s"大佬"))
        val smallStream: DataStream[(String, String)] = splitStream.select("new").map(e => (e.name, s"马仔"))

        val connectedStream: ConnectedStreams[(String, String), (String, String)] = bigStream.connect(smallStream)
        //connectedStream 不能直接打印
        connectedStream.map(
            big=>("name is"+big._1,"info is"+big._2),
            small=>("small is "+small._1,"info is "+small._2)
        ).print()

        env.execute("connect")


    }
}
