package cn.qphone.flink.day2

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.beans
import scala.beans.BeanProperty

object Demo3_DataStreamCustomSource_Mysql {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val mysqlStream: DataStream[Stu1] = env.addSource(new MysqlSourceFunction)
        mysqlStream.print().setParallelism(1)
        env.execute("mysql source")
    }
}
case class Stu1(id:Int,name:String)
class MysqlSourceFunction extends RichParallelSourceFunction[Stu1]{

    @BeanProperty var ps:PreparedStatement=_
    @BeanProperty var conn:Connection=_
    @BeanProperty var rs:ResultSet=_

    /**
     * 初始化
     * @param parameters
     */
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val driver="com.mysql.jdbc.Driver"
        val url="jdbc:mysql://192.168.10.100:3306/flink?useSSL=false"
        val username="root"
        val password="123456"
        Class.forName(driver)
        try{
            conn = DriverManager.getConnection(url,username,password)
            val sql="select * from stu1"
            ps = conn.prepareStatement(sql)
        }catch {
            case e:Exception=>e.printStackTrace()
        }

    }

    override def run(ctx: SourceFunction.SourceContext[Stu1]): Unit = {
        try{
            rs = ps.executeQuery()
            while(rs.next()){
                val stu1:Stu1=Stu1(rs.getInt("id"),rs.getString("name"))
                ctx.collect(stu1)
            }
        }catch {
            case e:Exception=>e.printStackTrace()
        }
    }

    override def cancel(): Unit = ???

    override def close(): Unit = {
     super.close()
        if(conn!=null) conn.close()
        if(ps!=null) ps.close()
        if(rs!=null) rs.close()
    }

}
