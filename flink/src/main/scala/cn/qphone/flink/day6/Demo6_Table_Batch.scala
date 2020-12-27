package cn.qphone.flink.day6

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Demo6_Table_Batch {
    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val tenv: BatchTableEnvironment = BatchTableEnvironment.create(env)
        val ds: DataSet[(Int, String, String, Int)] = env.fromElements("1,lixi,man,1").map(line => {
            val fields: Array[String] = line.split(",")
            (fields(0).toInt, fields(1), fields(2), fields(3).toInt)
        })

        val table: Table = tenv.fromDataSet(ds, 'id, 'name, 'sex, 'salary)
        table.groupBy("name")
          .select('name,'salary.sum as 'sum_age)
          .toDataSet[Row]
          .print()

        //env.execute()


    }
}
