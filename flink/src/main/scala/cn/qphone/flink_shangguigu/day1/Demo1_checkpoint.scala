package cn.qphone.flink_shangguigu.day1

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Demo1_checkpoint {
    //checkpoint代码
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.enableCheckpointing(1000L)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
        env.getCheckpointConfig.setCheckpointTimeout(60000L)
        //最多同时允许存在多少个checkpoint
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        //在两次checkpoint最小的间隔时间   设置这个相当于滑动窗口 ,会把上边那个最大并行度覆盖为1
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
        //默认为false，更喜欢用checkpoint还是save point
        env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
        //最多能容忍多少次checkpoint失败
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)
        //设置重新启动策略配置，配置指定了那个重新启动策略，将用于重新启动时的执行图
        //10秒之内重启三次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000l))
       //第一个参数:给定间隔内的最大重新启动次数 ,第二个参数:故障时间间隔 ，第三个参数:重新启动尝试之间的延迟
        env.setRestartStrategy(RestartStrategies.failureRateRestart(2,Time.of(5,TimeUnit.MINUTES),Time.of(5,TimeUnit.SECONDS)))
    }
}
