package cn.qphone.flink.day2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Demo1_Java_DataStreamSource {
    public static void main(String[] args) throws Exception {
        //1 获取上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.source

        //2.1 list source
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        DataStreamSource<Integer> dStream1 = env.fromCollection(list);
        SingleOutputStreamOperator<Integer> mapStream = dStream1.map(n -> n * 10);
        mapStream.print().setParallelism(1);
        System.out.println("===========================");

        //2.2 String Source
        DataStreamSource<String> dStream2 = env.fromElements("hello hello wdd");
        dStream2.print().setParallelism(1);
        System.out.println("===========================");

        //2.3 文件作为源
        DataStreamSource<String> dStream3 = env.readTextFile("file:///E:/data/hello.txt", "utf-8");
        dStream3.print().setParallelism(1);
        System.out.println("============================");

        //2.4 socket作为源
        DataStreamSource<String> dStream4 = env.socketTextStream("192.168.10.100", 6666);
        dStream4.print().setParallelism(1);
        System.out.println("============================");

        env.execute("java flink");

    }
}
