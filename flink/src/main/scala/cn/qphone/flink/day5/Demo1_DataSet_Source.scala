package cn.qphone.flink.day5

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.beans.BeanProperty

object Demo1_DataSet_Source {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._
        val text: DataSet[String] = env.fromElements("i love flink very much")
        val txt = text.flatMap(_.split("\\s+")).map((_, 1)).groupBy(0).sum(1).map(t => Wc(t._1, t._2))
        //1. 添加自定义outputformat
        txt.output(new BatchMysqlOutputFormat)

            env.execute()
    }
}

case class Wc(word:String, count:Int)

class BatchMysqlOutputFormat extends OutputFormat[Wc] {

    @BeanProperty var ps:PreparedStatement = _
    @BeanProperty var conn:Connection = _
    @BeanProperty var rs:ResultSet = _


    override def configure(parameters: Configuration): Unit = {

    }

    override def open(taskNumber: Int, numTasks: Int): Unit = {
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://192.168.10.100:3306/flink?useSSL=false"
        val username = "root"
        val password = "123456"
        Class.forName(driver)
        try {
            conn = DriverManager.getConnection(url, username, password)
        } catch {
            case e:Exception => e.printStackTrace()
        }
    }

    override def writeRecord(record: Wc): Unit = {
        ps = conn.prepareStatement("insert into wc values(?, ?)")
        ps.setString(1, record.word)
        ps.setInt(2, record.count)
        ps.execute()
    }

    override def close(): Unit = {
        if (conn != null) conn.close()
        if (ps != null) ps.close()
        if (rs != null) rs.close()
    }
}
