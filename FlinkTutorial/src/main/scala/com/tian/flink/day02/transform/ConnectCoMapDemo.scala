package com.tian.flink.day02.transform

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author tian
 * @date 2019/10/23 15:00
 * @version 1.0.0
 */
object ConnectCoMapDemo {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputStream = env.socketTextStream("localhost", 7777)
        import org.apache.flink.streaming.api.scala._
        val sensorStream = inputStream
            .map(data => {
                val dataArr = data.split(",")
                SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
            })
        val splitStream = sensorStream
            .split(data => if (data.temp > 30) Seq("high") else Seq("low"))
        val low = splitStream.select("low")
        val high = splitStream.select("high")
        val warning = high.map(data => (data.id, data.temp))
        val connectedStream = warning.connect(low)
        val coMapStream = connectedStream.map(
            warningData => (warningData._1, warningData._2, "warning"),
            lowData => (lowData.id, "healthy")
        )
        coMapStream.print()
        env.execute()
    }
}
