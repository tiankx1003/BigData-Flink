package com.tian.flink.day02.transform

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * @author tian
 * @date 2019/10/23 14:44
 * @version 1.0.0
 */
object SplitSelectDemo {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputStream = env.socketTextStream("localhost",7777)
        val splitStream = inputStream
            .map(data => {
                val dataArr = data.split(",")
                SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
            })
            .split(sensorData => if (sensorData.temp > 30) Seq("high") else Seq("low"))
        val low = splitStream.select("low")
        val high = splitStream.select("high")
        val all = splitStream.select("low","high")
        low.print()
        env.execute()
    }
}
