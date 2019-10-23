package com.tian.flink.day03.window

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author tian
 * @date 2019/10/23 21:09
 * @version 1.0.0
 */
object CountWindowDemo {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val socketSource: DataStream[String] = env.socketTextStream("localhost", 7777)
        import org.apache.flink.streaming.api.scala._
        val sensorStream: DataStream[SensorReading] = socketSource.map(data => {
            val splits: Array[String] = data.split(",")
            SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
        })
        sensorStream.map(r => (r.id, r.temp))
            .keyBy(_._1)
            //.countWindow(5) //窗口长度为5
            .countWindow(6,3) //窗口长度为6，滑动步长为3
            .reduce((r1, r2) => (r1._1, r1._2.max(r2._2)))
            .print()
        env.execute()
    }
}
