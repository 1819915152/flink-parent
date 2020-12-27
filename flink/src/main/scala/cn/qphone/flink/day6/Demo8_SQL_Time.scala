package cn.qphone.flink.day6

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

object Demo8_SQL_Time {
    def main(args: Array[String]): Unit = {
        //1.获取到流式的环境对象
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //2.获取到table的环境对象
        val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)
        //3.使用流式的环境对象获取到source的数据
        val socket: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)
        val data: DataStream[DPAT] = socket.map(line => {
            val fields: Array[String] = line.split("\\s+")
            val date: String = fields(0).trim
            val province: String = fields(1)
            val add: Int = fields(2).toInt
            val ts: Long = fields(3).toLong
            DPAT(date + "_" + province, add, ts)
        })
        //4.将DataStream转换为一个table对象
        // 将data数据转换为table的过程中并给其中每个元组对应的元素赋值(别名)
        val table: Table = tenv.fromDataStream[DPAT](data)
        //5.sql:tumble(时间值，间隔时间) ,一般再group by后面用
        tenv.sqlQuery(
            s"""
               |select
               |dp,
               |sum(add) as sum_cnt
               |from
               |$table
               |group by dp
               |""".stripMargin).toRetractStream[Row].print()

        //7.执行
        env.execute()

    }
}
case class DPAT(dp:String,add:Int,ts:Long)