package cn.qphone.flink.day4

import cn.qphone.flink.day3.ObtainEmployee
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Demo1_Sink_Redis {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val obtainEmploymentStream: DataStream[ObtainEmployee] = env.socketTextStream("192.168.10.100", 6666)
          .map(line => {
              val obtainEmployment: Array[String] = line.split("\\s+")
              println(obtainEmployment.mkString(","))
              ObtainEmployee(obtainEmployment(0).toInt, obtainEmployment(1), obtainEmployment(2).toDouble, obtainEmployment(3))
          })
        val tStream: DataStream[(String, String)] = obtainEmploymentStream.map(oe => (oe.name, oe.address))
        tStream.print().setParallelism(1)

        //创建redis sink
        val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
          .setHost("192.168.10.100")
          .setPort(6379)
          .build()

        val sink = new RedisSink(config, new MyRedisSink)

        tStream.addSink(sink)
        env.execute("redis sink")

    }
}

/**
 * 自定义 redis sink
 */
class MyRedisSink extends RedisMapper[(String,String)]{
    /**
     *
     * @return
     */
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.SET,null)
    }

    /**
     * key
     * @param t
     * @return
     */
    override def getKeyFromData(t: (String, String)): String = {
        return t._1
    }

    /**
     * value
     * @param t
     * @return
     */
    override def getValueFromData(t: (String, String)): String = {
        return t._2
    }
}