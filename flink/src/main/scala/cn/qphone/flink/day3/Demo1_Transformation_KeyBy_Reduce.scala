package cn.qphone.flink.day3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * keyBy:DataStream-->KeyedStream
 * 作用：将具有相同的key的数据分配到一个区中，内部使用的是散列分区，类似于sql中的group by,后序获取到KeyedStream的操作
 *      都是基于组内的操作
 *
 * reduce：聚合
 * 作用:将数据合并成为一个新的数据，返回单个结果值。并且reduce在处理我们的数据的时候总是会创建一个新的值。
 *
 *
 */
object Demo1_Transformation_KeyBy_Reduce {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val reduceStream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 33), Tuple2(100, 66), Tuple2(100, 56), Tuple2(200, 666))
          .keyBy(0) //按照第一个元素来进行分组
          //.sum(1)
          //.min(1)
          //.max(1)
          .reduce((a, b) => (a._1, a._2 + b._2))

        reduceStream.print().setParallelism(1)
        env.execute("keyBy_Reduce")

    }

}
