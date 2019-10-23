package com.tian.flink.day02.sink

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * Redis Sink
 *
 * @author tian
 * @date 2019/10/23 20:19
 * @version 1.0.0
 */
object RedisSinkDemo {
    def main(args: Array[String]): Unit = {
        val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val socketSource = env.socketTextStream("localhost", 7777)
        import org.apache.flink.streaming.api.scala._
        val daaStream = socketSource.map(data => {
            val splits = data.split(",")
            SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
        })
        daaStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))
        env.execute()
    }
}

class MyRedisMapper extends RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription =
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")

    override def getKeyFromData(t: SensorReading): String = t.id

    override def getValueFromData(t: SensorReading): String = t.temp.toString
}