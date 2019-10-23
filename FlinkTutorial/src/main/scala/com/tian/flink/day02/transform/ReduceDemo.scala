package com.tian.flink.day02.transform

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @author tian
 * @date 2019/10/23 14:24
 * @version 1.0.0
 */
object ReduceDemo {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env
            .socketTextStream("localhost",7777)
            .map(data => {
                val dataArr = data.split(",")
                SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
            })
            .keyBy("id")
            .reduce((x, y) => SensorReading(x.id, x.ts + 1, y.temp))
            //.filter(_.id=="sensor_2")
            .print
        env.execute()
    }
}
