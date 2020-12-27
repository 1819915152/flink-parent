package cn.qphone.flink.day6

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object Demo5_Table_QuickStart2 {
    def main(args: Array[String]): Unit = {
        //1.获取到流式的环境对象
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._
        //2.获取到table的环境对象
        val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)
        //3.使用流式的环境对象获取到source的数据
        val socket: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)
        val data: DataStream[(String, Int)] = socket.map(line => {
            val fields: Array[String] = line.split("\\s+")
            val date: String = fields(0).trim
            val province: String = fields(1)
            val add: Int = fields(2).toInt
            (date + "_" + province, add)
        })
        //4.将DataStream转换为一个table对象
        import org.apache.flink.table.api.scala._ //导入一个flink table的隐式转换函数

        var table: Table = tenv.fromDataStream(data,'date_province,'cnt)
        //5.使用table来查询数据
        table=table.select("date_province,cnt").where("cnt>2")
        table.printSchema()
        //6.将table转换为DataStream然后输出
        tenv.toAppendStream[Row](table).print("table ->")
        //7.执行
        env.execute()

    }
}
