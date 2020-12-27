package cn.qphone.flink.day5

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object Dem3_State {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._
        env.fromElements((1,5), (1, 6), (1, 7), (2, 8), (2, 1))
          .keyBy(0)
          .flatMap(new MyFlatMapFunction)
          .print

        env.execute()
    }
}

/**
 * 自定义State
 * * 每个算子起始都有对应*Function或Rich*Function
 * * 一般我们在自定义的时候都会继承这个函数对应的富函数（AbstractRichFunction）
 * * 实现这个富函数中的抽象方法
 */
class MyFlatMapFunction extends RichFlatMapFunction[(Int,Int), (Int,Int)] {

    var state:ValueState[(Int, Int)] = _

    /**
     * 初始化
     */
    override def open(parameters: Configuration): Unit = {
        val descriptor = new ValueStateDescriptor[(Int, Int)](
            "avg",
            TypeInformation.of(new TypeHint[(Int, Int)] {}),
            (0, 0)
        )
        state = getRuntimeContext.getState(descriptor) // int sum = 0
    }

    override def flatMap(value: (Int, Int), out: Collector[(Int, Int)]): Unit = {
        // 获取到当前的状态
        val currentState: (Int, Int) = state.value() // sum - 0
        val count: Int = currentState._1 + 1 // 1
        val sum: Int = currentState._2 + value._2 // 0+1

        //更新状态
        state.update((count, sum)) // sum=0+1

        //输出状态
        out.collect(value._1, sum)

    }

    /**
     * 释放资源
     */
    override def close(): Unit = super.close()
}
