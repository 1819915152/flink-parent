package cn.qphone.flink.day3

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.beans.BeanProperty
import org.apache.flink.api.scala._

/**
 * 1 liujiahao 30000 shanghai
 * 2 张辉 23000 北京
 * 3 程志远 25000 杭州
 */
object Demo3_JDBC {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val obtainEmploymentStream: DataStream[ObtainEmployee] = env.socketTextStream("192.168.10.100", 6666)
          .map(line => {
              val obtainEmployment: Array[String] = line.split("\\s+")
              println(obtainEmployment.mkString(","))
              ObtainEmployee(obtainEmployment(0).toInt, obtainEmployment(1), obtainEmployment(2).toDouble, obtainEmployment(3))
          })
        obtainEmploymentStream.print().setParallelism(1)
        obtainEmploymentStream.writeUsingOutputFormat(new MysqlOutputFormat)

        env.execute("JDBC")
    }
}
case class ObtainEmployee(id:Int,name:String,salary:Double,address:String)

class MysqlOutputFormat extends OutputFormat[ObtainEmployee]{

    @BeanProperty var ps:PreparedStatement=_
    @BeanProperty var conn:Connection=_
    @BeanProperty var rs:ResultSet=_

    /**
     * 用于配置相关的初始化
     * @param configuration
     */
    override def configure(configuration: Configuration): Unit = {

    }

    /**
     *  业务初始化
     * @param i
     * @param i1
     */
    override def open(i: Int, i1: Int): Unit = {
        val driver="com.mysql.jdbc.Driver"
        val url="jdbc:mysql://192.168.10.100:3306/flink?useSSL=false"
        val username="root"
        val password="123456"

        Class.forName(driver)
        try{
            conn=DriverManager.getConnection(url,username,password)
        }catch {
            case e:Exception=>e.printStackTrace()
        }

    }

    /**
     * 写记录
     * @param record
     */
    override def writeRecord(record: ObtainEmployee): Unit = {
        ps = conn.prepareStatement("insert into obtain_employment values(?,?,?,?)")
        ps.setInt(1,record.id)
        ps.setString(2,record.name)
        ps.setDouble(3,record.salary)
        ps.setString(4,record.address)
        ps.execute()
    }

    /**
     * 最后被调用
     */
    override def close(): Unit = {
        if(conn!=null) conn.close()
        if(ps!=null) ps.close()
        if(rs!=null) rs.close()
    }
}