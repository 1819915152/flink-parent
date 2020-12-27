package cn.qphone.flink.day5

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.collection.mutable.Map

/**
 * gender.txt:1 男,2 女
 */
object Demo6_Distribute_cache {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._

        //读取hdfs的资源并设置分布式缓存
        env.registerCachedFile("file:///E:/data/gender.txt", "info")

        env.socketTextStream("192.168.10.100", 6666)
          .map(new RichMapFunction[String, (String,String)] {

              var bc:BufferedSource = _
              val map: mutable.Map[Int, String] = Map() // 做的缓存

              override def open(parameters: Configuration): Unit = {
                  //读取分布式缓存中的数据
                  val file: File = getRuntimeContext.getDistributedCache.getFile("info") // 获取分布式缓存中的数据
                  bc = Source.fromFile(file)
                  val list: List[String] = bc.getLines().toList
                  for(line <- list) {
                      val fields: Array[String] = line.split("\\s+")
                      val sexid: Int = fields(0).toInt
                      val sex: String = fields(1)
                      map.put(sexid, sex)
                  }
              }
              override def map(line: String): (String, String) = {
                  val fields: Array[String] = line.split("\\s+")
                  val name: String = fields(0)
                  val sexid: Int = fields(1).toInt
                  val sex: String = map.getOrElse(sexid, "妖")
                  (name, sex)
              }

              override def close(): Unit = {
                  if(bc != null) bc.close()
              }
          }).print()
        env.execute()
    }
}