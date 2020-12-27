package cn.qphone.flink.day5

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Demo4_RocketsDB_Backend {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._
        //获取checkpoint的配置对象
        val config: CheckpointConfig = env.getCheckpointConfig

        /**
         * DELETE_ON_CANCELLATION:取消作业的时候删除checkpoint
         * RETAIN_ON_CANCELLATION:取消作业的时候保留checkpoint
         */
        config.enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

        // 设置EXACTLY_ONCE模式
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

        // 设置每个检查点最小间隔时间1s
        config.setMinPauseBetweenCheckpoints(1000)

        // 设置每次checkpoint快照必须在1分钟之内结束
        config.setCheckpointTimeout(60000)

        // 设置在统一时间范围内只允许一个检查点
        config.setMaxConcurrentCheckpoints(1)

        val data = env.fromElements("i love you very much").print()

        env.execute()
    }
}