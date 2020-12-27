package cn.qphone.flink.day5

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Demo1_WordCount_Scala {
    def main(args: Array[String]): Unit = {
        //2. 获取流式执行环境：批式执行环境ExecutionEnvironment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //3. 导入隐式参数
        import org.apache.flink.api.scala._

        //4. 计算
        //map操作符不能往前链接，但是往后链接。说白了就是map和print连接到一起
        env.fromElements("i love you").map((_,1)).startNewChain().print()

        //map操作符不能再链接到其他的操作符了——禁止链接map操作符
        env.fromElements("i hate you").map((_,1)).disableChaining().print()

        //map操作符放入到指定的task共享组中去共享task slot
        env.fromElements("i hate you").map((_,1)).slotSharingGroup("default")
        //6. 触发执行
        env.execute("wordcount scala")
    }
}
