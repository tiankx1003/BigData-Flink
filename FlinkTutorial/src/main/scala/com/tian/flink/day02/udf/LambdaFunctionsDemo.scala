package com.tian.flink.day02.udf

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 匿名函数
 *
 * @author tian
 * @date 2019/10/23 15:32
 * @version 1.0.0
 */
object LambdaFunctionsDemo {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val socketSource = env.socketTextStream("localhost", 7777)
        val lambdaStream = socketSource.filter(_.contains("scala"))
        lambdaStream.print()
        env.execute()
    }
}
