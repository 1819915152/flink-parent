package cn.qphone.flink.day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Demo1_Wordcount_Java {
    public static void main(String[] args) throws Exception {
        //1. 参数准备
        int port = 6666;
        try {
            //1. 获取到参数工具类，作用加载你传递的参数
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e) {
            e.printStackTrace();
            System.err.println("no port set, default:port is 6666");
            port = 6666;
        }

        String hostname = "192.168.10.100";

        //2. 获取到编程的入口 (流程序执行的上下文)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //3. 通过web socket:获取到数据
        DataStreamSource<String> data = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<WordWithCount> pairWords = data.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String line, Collector<WordWithCount> out) throws Exception {
                String[] split = line.split("\\s+");
                for (String word : split) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        });
        KeyedStream<WordWithCount, Tuple> grouped = pairWords.keyBy("word");
        WindowedStream<WordWithCount, Tuple, TimeWindow> window = grouped.timeWindow(Time.seconds(2), Time.seconds(1));
//        SingleOutputStreamOperator<WordWithCount> cnts = window.sum("count");
        SingleOutputStreamOperator<WordWithCount> cnts = window.reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount t1, WordWithCount t2) throws Exception {
                return new WordWithCount(t1.word, t1.count + t2.count);
            }
        });
        cnts.print().setParallelism(1);

        env.execute("wordcount");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
