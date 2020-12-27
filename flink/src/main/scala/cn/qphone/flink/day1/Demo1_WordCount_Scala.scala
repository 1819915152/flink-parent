package cn.qphone.flink.day1

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


object Demo1_WordCount_Scala {
  def main(args: Array[String]): Unit = {
    //1. 准备参数
    var port:Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e:Exception => System.err.println("no port set, default:port is 6666")
      6666
    }

    //2. 获取数据
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = env.socketTextStream("192.168.10.100", 6666)

    //3. 导入隐式参数
    import org.apache.flink.api.scala._

    //4. 计算
    val cnts: DataStream[WordWithScalaCount] = data.flatMap(_.split("\\s+")).map(WordWithScalaCount(_, 1)).keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(2))
      .sum("count")
     // .reduce((a, b) => WordWithScalaCount(a.word, a.count + b.count))

    //5. 结果打印
    cnts.print().setParallelism(1)

    //6. 执行
    env.execute("wordcount scala")
  }

  case class WordWithScalaCount(word:String, count:Int)
}
