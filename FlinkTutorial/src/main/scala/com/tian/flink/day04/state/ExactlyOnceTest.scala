package com.tian.flink.day04.state

import java.util.Properties

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

/**
 * 状态一致性的代码实现
 *
 * @author tian
 * @date 2019/10/23 9:21
 * @version 1.0.0
 */
object ExactlyOnceTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.enableCheckpointing(60000L) //打开检查点支持

        val properties: Properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")
        val inputStream: DataStream[String] =
            env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
        val dataStream: DataStream[String] = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble).toString
            })
        dataStream.addSink(new FlinkKafkaProducer011[String](
            "exactly-once test",
            new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),
            properties,
            Semantic.EXACTLY_ONCE //默认状态一致性为AT_LEAST_ONCE
        ))
        dataStream.print()
        env.execute("exactly-once test")
        /*
        kafka consumer 配置isolation.level 改为read_committed，默认为read_uncommitted，
        否则未提交(包括预提交)的消息会被消费走，同样无法实现状态一致性
         */
    }
}
