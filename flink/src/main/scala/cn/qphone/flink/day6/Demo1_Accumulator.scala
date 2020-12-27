package cn.qphone.flink.day6

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Demo1_Accumulator {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._

        val data = env.fromElements("i love you very much")

        val res: DataStream[String] = data.map(new RichMapFunction[String, String] {

            var intCounter: IntCounter = _

            override def open(parameters: Configuration): Unit = {
                //1.创建一个累加
                intCounter = new IntCounter(0)
                //2.注册累加器
                getRuntimeContext.addAccumulator("recordCounter", intCounter)
            }

            //累计输入记录
            override def map(value: String): String = {
                if (value != null) {
                    intCounter.add(1)
                }
                //返回
                intCounter + "_" + value
            }
        })

        res.print("====")
        //获取累加器的值 --- 只能任务执行结束才可以看到
        //5.触发执行
        val accres: JobExecutionResult = env.execute("cache")
        val accvalue: String = accres.getAccumulatorResult("recordCounter").toString
        println("累加器最终结果:"+accvalue)

    }
}
//class MyFlatMapFunction extends RichFlatMapFunction[String,String]{
//    private var env:StreamExecutionEnvironment=_
//
//    def this(env:StreamExecutionEnvironment){
//        this
//        this.env=env
//    }
//
//    override def flatMap(value: String, out: Collector[String]): Unit = {
//
//        //1.创建累加器
//        val counter = new IntCounter
//        //2.注册累加器
//        getRuntimeContext.addAccumulator("count_name",counter)
//        //3.当达成了某种条件
//        counter.add(1)
//    }
//}