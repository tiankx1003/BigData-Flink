package com.tian.flink.day02.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author tian
 * @date 2019/10/23 20:40
 * @version 1.0.0
 */
object JdbcDemo {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val socketSource = env.socketTextStream("localhost", 7777)
        import org.apache.flink.streaming.api.scala._
        val sensorStream = socketSource.map(data => {
            val splits = data.split(",")
            SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
        })
        sensorStream.addSink(new MyJdbcSink)
        env.execute()
    }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    /**
     * 主要用于创建连接
     *
     * @param parameters
     */
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
        insertStmt = conn.prepareStatement("insert into temperatures (sensor,temp) value (?,?)")
        updateStmt = conn.prepareStatement("update temperature set temporary = ? where sensor = ?;")
    }

    /**
     * 调用连接执行sql
     *
     * @param value
     * @param context
     */
    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
        updateStmt.setDouble(1, value.temp)
        updateStmt.setString(2, value.id)
        updateStmt.execute()
        if (updateStmt.getUpdateCount == 0) {
            insertStmt.setString(1, value.id)
            insertStmt.setDouble(2, value.temp)
            insertStmt.execute()
        }
    }

    /**
     * 关闭资源
     */
    override def close(): Unit = {
        insertStmt.close()
        updateStmt.close()
        conn.close()
    }
}