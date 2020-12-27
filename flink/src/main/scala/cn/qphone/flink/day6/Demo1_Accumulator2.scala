package cn.qphone.flink.day6

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import sun.management.counter.LongCounter

object Demo1_Accumulator2 {
    def main(args: Array[String]): Unit = {
        //1.获取到流式的环境对象
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //2.
        val socket: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)
        socket.map(new RichMapFunction[String,String] {

            //1.声明累加器，要在复函数中声明全局的
            val normal = new IntCounter()
            val exception = new IntCounter()
            val all=new IntCounter()

            //2.在open函数中添加累加器
            override def open(parameters: Configuration): Unit = {
                val rc: RuntimeContext = getRuntimeContext
                rc.addAccumulator("normal_temperature",normal)
                rc.addAccumulator("exception_temperature",exception)
                rc.addAccumulator("all_temperature",all)
            }

            override def map(value: String): String = {
                //进行累加
                all.add(1)
                val msg="体温过高,需要隔离进行排查~"
                val temper: Double = value.split(",")(3).toDouble
                if(temper>36.4) {
                    //进行累加
                    exception.add(1)
                    s"${value.split(',')(1).trim} 的体温为 $temper, $msg"
                }else{
                    //进行累加
                    normal.add(1)
                    s"${value.split(',')(1).trim} 的体温为 $temper, 体温正常，请进入"
                }

            }
        }).print("体温结果为:")

        val result: JobExecutionResult = env.execute("accumulator")

        //3.执行完了之后获取累加器的值
        val normal: Double = result.getAccumulatorResult[Double]("normal_temperature")
        val exception: Double = result.getAccumulatorResult[Double]("exception_temperature")
        val all: Double = result.getAccumulatorResult[Double]("all_temperature")

        println(s"normal $normal")
        println(s"exception $exception")
        println(s"all $all")
    }
}
