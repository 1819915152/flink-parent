package cn.qphone.flink.day5

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object Demo2_Partitioner {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val data = env.fromElements("i love you very much")
        data.shuffle.print("shuffle-->").setParallelism(4) // ShufflePartitoner
        data.rescale.print("rescale-->").setParallelism(4) // RescalePartitoner
        data.rebalance.print("rebalance-->").setParallelism(4)
        env.execute()
    }
}