package com.tian.flink.day02.udf

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * 富函数
 *
 * @author tian
 * @date 2019/10/23 15:35
 * @version 1.0.0
 */
object RichFunctionsDemo {
    def main(args: Array[String]): Unit = {

    }
}

// TODO: 多种富函数的实现
class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {
    var subTaskIndex = 0

    override def open(configuration: Configuration): Unit = {
        subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
        // 以下可以做一些初始化工作，例如建立一个和HDFS的连接
    }

    override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
        if (in % 2 == subTaskIndex) {
            out.collect((subTaskIndex, in))
        }
    }

    override def close(): Unit = {
        // 以下做一些清理工作，例如断开和HDFS的连接。
    }

}
