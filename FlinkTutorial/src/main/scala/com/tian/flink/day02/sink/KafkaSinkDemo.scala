package com.tian.flink.day02.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * Kafka Sink
 *
 * @author tian
 * @date 2019/10/23 16:06
 * @version 1.0.0
 */
object KafkaSinkDemo {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val socketSource = env.socketTextStream("localhost", 7777)
        socketSource.addSink(new FlinkKafkaProducer011[String](
            "localhost:9092",
            "test",
            new SimpleStringSchema())
        )
        env.execute()
    }
}
