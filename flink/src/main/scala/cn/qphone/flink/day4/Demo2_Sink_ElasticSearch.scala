package cn.qphone.flink.day4

import java.util

import cn.qphone.flink.day3.ObtainEmployee
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object Demo2_Sink_ElasticSearch {
    def main(args: Array[String]): Unit = {
        //1. source
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val obtainEmploymentStream: DataStream[ObtainEmployee] = env.socketTextStream("192.168.10.100", 6666)
          .map(line => {
              val obtainEmployment: Array[String] = line.split("\\s+")
              println(obtainEmployment.mkString(","))
              ObtainEmployee(obtainEmployment(0).toInt, obtainEmployment(1), obtainEmployment(2).toDouble, obtainEmployment(3))
          })

        obtainEmploymentStream.print().setParallelism(1)

        //2. 整合es sink
        //2.1 集合指定es的位置
        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("192.168.10.100", 9200, "http"))
        //2.2 获取到sink对象
        val sink = new ElasticsearchSink.Builder[ObtainEmployee](httpHosts, new MyEsSink).build()
        //2.3 添加sink
        obtainEmploymentStream.addSink(sink)

        env.execute("sind 2 es")
    }
}

class MyEsSink extends ElasticsearchSinkFunction[ObtainEmployee] {

    /**
     * 当当前DataStream中每流动一个元素，此方法调用一次
     * @param element 数据
     * @param runtimeContext
     * @param requestIndexer
     */
    override def process(element: ObtainEmployee, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //1. 将javabean对象的数据封装到java的map中
        println(s"$element")
        val map = new util.HashMap[String, String]()
        map.put("name", element.name)
        map.put("address", element.address)
        //2. 将map中的数据构造一个IndexRequest请求
        val request: IndexRequest = Requests.indexRequest()
          .index("flink") // 索引库
          .`type`("info") // 索引类型
          .id(s"${element.id}") // docid
          .source(map) // 数据
        //3. 将索引请求对象传递给请求索引器
        requestIndexer.add(request)
    }
}