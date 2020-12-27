package cn.qphone.flink.day2

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

/**
 *  自定义DataStream的Source
 *  1.继承SourceFunction:并非行，不能指定其并行度，不能指定setParallelism(1),如：socketTextStreamFunction
 *  2.继承ParallelSourceFunction:是一个并行的SourceFunction，可以指定并行度。
 *  3.继承RichParallelSourceFunction:实现了ParallelSourceFunction:不但能够并行，还有其他功能，比如增加了open和close、getRuntimeContext...
 *
 *
 */
object Demo2_DataStreamCustomSource {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //1.添加自定义的source
        import org.apache.flink.api.scala._
        val dataStream1: DataStream[String] = env.addSource(new MySourceFunction)

        dataStream1.print().setParallelism(1)

        env.execute("custom source")

    }
}
class MySourceFunction extends SourceFunction[String]{
    /**
     * 向下游产生数据
     * @param ctx
     */
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random()
        while(true){
            val num:Int = random.nextInt(100)
            ctx.collect(s"random:${num}")
            Thread.sleep(500)
        }
    }

    /**
     * 取消，用于控制run方法的结束
     */
    override def cancel(): Unit = {

    }
}
class MyRichParallelSourceFunction extends RichParallelSourceFunction[String]{
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random()
        while(true){
            val num:Int = random.nextInt(100)
            ctx.collect(s"random:${num}")
            Thread.sleep(500)
        }
    }

    override def cancel(): Unit = ???

    /**
     * 初始化方法
     * @param parameters
     */
    override def open(parameters: Configuration): Unit = super.open(parameters)

    /**
     * 适合在关闭的时候调用
     */
    override def close(): Unit = super.close()
}