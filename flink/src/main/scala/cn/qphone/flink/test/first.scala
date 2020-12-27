package cn.qphone.flink.test

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object first {
    def main(args: Array[String]): Unit = {
        import org.apache.flink.api.scala._
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)
        val textStream: DataStream[String] = env.readTextFile("flink/src/data/first")
        //val textStream: DataStream[String] = env.socketTextStream("192.168.10.100", 9999)
        val word= textStream.map(x => {
            val fields: Array[String] = x.split(",")
            (fields(0), fields(1).toInt)
        })
        val table: Table = tenv.fromDataStream(word,'channel,'uid)
        tenv.sqlQuery(
            s"""
              |select channel,count(uid)
              |from $table
              |group by channel
              |""".stripMargin).toRetractStream[Row].print()


        env.execute()

    }
}
case class channel(channel:String,uid:Int)