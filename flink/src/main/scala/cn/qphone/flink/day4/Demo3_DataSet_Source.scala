package cn.qphone.flink.day4

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object Demo3_DataSet_Source {
    def main(args: Array[String]): Unit = {
        //1.获取到入口类
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        val ds2: DataSet[String] = env.fromElements("lixi", "rock", "lee")
        ds2.print()

        val ds3: DataSet[Long] = env.generateSequence(1, 100)
        ds3.print()
        //2.执行
        env.execute("source batch")
    }
}
