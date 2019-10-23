package com.tian.flink.day02.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable
import scala.util.Random

/**
 * @author tian
 * @date 2019/10/20 13:23
 * @version 1.0.0
 */
class MySensorSource extends SourceFunction[SensorReading] { // TODO: 为什么继承SourceFunction要指定泛型?
    var isRunning: Boolean = true

    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
        //初始化一个随机数发生器
        val rand: Random = new Random()
        val curTemp: immutable.Seq[(String, Double)] = //十组随机数温度和对应id组成的元组
            1.to(10).map(i => ("sensor_" + i, 65 + rand.nextGaussian() * 20))
        while (isRunning) {
            //更新温度值
            curTemp.map(t => (t._1, t._2 + rand.nextGaussian()))
            val ts: Long = System.currentTimeMillis() //获取时间戳
            curTemp.foreach(t => sourceContext.collect(SensorReading(t._1, ts, t._2))) //加入时间戳
            Thread.sleep(100)

        }
    }

    override def cancel(): Unit = isRunning = false
}
