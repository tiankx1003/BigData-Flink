package com.tian.flink.day01.wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 批处理wordcount
 *
 * @author tian
 * @date 2019/10/18 10:00
 * @version 1.0.0
 */
object BatchWordCount {
    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        // 从文件中读取数据
        val inputDS: DataSet[String] = env.readTextFile(args(0))
        // 分词之后，对单词进行groupby分组，然后用sum进行聚合
        val wordCountDS: AggregateDataSet[(String, Int)] = inputDS
            .flatMap(_.split(" ")) //引入隐式转换
            .map((_, 1))
            .groupBy(0) //.groupBy(_._1)
            .sum(1)

        // 打印输出
        wordCountDS.writeAsCsv(args(1))
        env.execute()
        //批处理中使用的为有界的流，不需要手动关闭资源
    }

}
