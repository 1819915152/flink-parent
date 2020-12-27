package cn.qphone.flink.test


import org.apache.calcite.schema.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment

object second {
    def main(args: Array[String]): Unit = {
        import org.apache.flink.api.scala._
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)
        val textStream: DataStream[String] = env.readTextFile("flink/src/data/first")
        textStream.map(x=>{
            val fields: Array[String] = x.split(",")
            (fields(0),1)
        }).keyBy(0)
            .timeWindow(Time.minutes(5))
            .sum(1)

        env.execute()
    }
}