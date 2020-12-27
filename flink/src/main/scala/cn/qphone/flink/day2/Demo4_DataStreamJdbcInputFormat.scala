package cn.qphone.flink.day2

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

object Demo4_DataStreamJdbcInputFormat {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //1.设置inputFormat
        //1.1 创建rowTypeInfo
        val fieldsType: Array[TypeInformation[_]] = Array[TypeInformation[_]](
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
        )

        val rowTypeInfo = new RowTypeInfo(fieldsType: _*)
        //1.2 定义jdbcInputFormat
        val jDBCInputFormat: JDBCInputFormat = JDBCInputFormat.buildJDBCInputFormat()
          .setDBUrl("jdbc:mysql://192.168.10.100:3306/flink?useSSL=false")
          .setDrivername("com.mysql.jdbc.Driver")
          .setUsername("root")
          .setPassword("123456")
          .setQuery("select * from stu1")
          .setRowTypeInfo(rowTypeInfo)
          .finish()

        val jdbcStream: DataStream[Row] = env.createInput(jDBCInputFormat)
        jdbcStream.print().setParallelism(1)

        env.execute("jdbc")

    }
}
