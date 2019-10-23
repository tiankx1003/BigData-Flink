package com.tian.flink.day02.udf

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 函数类
 *
 * @author tian
 * @date 2019/10/23 15:21
 * @version 1.0.0
 */
class MyFilter extends FilterFunction[String] {
    override def filter(t: String): Boolean = t.contains("flink")
}

/**
 * 可以设置传参
 *
 * @param keyWord 关键字
 */
class KeyWord(keyWord: String) extends FilterFunction[String] {
    override def filter(t: String): Boolean = t.contains(keyWord)
}

object FunctionClassDemo {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val socketSource = env.socketTextStream("localhost", 7777)
        val filterStream = socketSource.filter(new MyFilter)
        val filterStream2 = socketSource.filter(new RichFilterFunction[String] { //通过匿名内部类是实现
            override def filter(t: String): Boolean = t.contains("spark")
        })
        val filterStream3 = socketSource.filter(new KeyWord("scala"))
        filterStream2.print()
        env.execute()
    }
}
