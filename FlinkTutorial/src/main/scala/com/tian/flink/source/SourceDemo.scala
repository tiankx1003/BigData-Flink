package com.tian.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * source
 *
 * @author tian
 * @date 2019/10/20 11:47
 * @version 1.0.0
 */
object SourceDemo {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val sensorList: List[SensorReading] = List(
            SensorReading("sensor_1", 1547718199, 35.80018327300259),
            SensorReading("sensor_6", 1547718201, 15.402984393403084),
            SensorReading("sensor_7", 1547718202, 6.720945201171228),
            SensorReading("sensor_10", 1547718205, 38.101067604893444)
        )
        import org.apache.flink.streaming.api.scala._
        //从集合中获取数据
        val listSource: DataStream[SensorReading] = env.fromCollection(sensorList)
        //从文件中获取数据
        val fileSource: DataStream[String] = env.readTextFile("file")
        //从kafka中获取数据
        val properties: Properties = new Properties() // TODO: properties可以写入文件
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")
        val kafkaSource: DataStream[String] =
            env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
        //从自定义source中获取数据
        val mySource: DataStream[SensorReading] = env.addSource(new MySensorSource())
        listSource.print.setParallelism(1)
        //fileSource.print.setParallelism(1)
        //kafkaSource.print.setParallelism(1)
        //mySource.print.setParallelism(1)
        env.execute()
    }
}

/**
 * 传感器样例类
 *
 * @param id   传感器id
 * @param ts   时间戳
 * @param temp 温度值
 */
case class SensorReading(id: String, ts: Long, temp: Double)