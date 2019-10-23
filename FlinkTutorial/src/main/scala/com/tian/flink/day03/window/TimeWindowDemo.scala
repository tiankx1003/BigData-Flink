package com.tian.flink.day03.window

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author tian
 * @date 2019/10/23 21:00
 * @version 1.0.0
 */
object TimeWindowDemo {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val socketSource: DataStream[String] = env.socketTextStream("localhost", 7777)
        import org.apache.flink.streaming.api.scala._
        val sensorStream: DataStream[SensorReading] = socketSource.map(data => {
            val splits: Array[String] = data.split(",")
            SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
        })
        val windowDS1: DataStream[(String, Double)] = sensorStream.map(r => (r.id, r.temp))
            .keyBy(_._1)
            //窗口长度为15秒
            .timeWindow(Time.seconds(15))
            .reduce((r1, r2) => (r1._1, r2._2.min(r1._2)))
        val windowDS2: DataStream[(String, Double)] = sensorStream.map(r => (r.id, r.temp))
            .keyBy(_._1)
            //窗口长度为15秒，滑动步长为5秒
            .timeWindow(Time.seconds(15), Time.seconds(5))
            .reduce((r1, r2) => (r1._1, r2._2.min(r1._2)))
        windowDS1.print()
        env.execute()
    }
}
