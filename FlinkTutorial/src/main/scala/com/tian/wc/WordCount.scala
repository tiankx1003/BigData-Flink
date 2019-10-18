package com.tian.wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
 * @author tian
 * @date 2019/10/18 10:00
 * @version 1.0.0
 */
class WordCount {
    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = ExecutionEnvironment.getExecutionEnvironment
        // 从文件中读取数据
        val inputPath = "files/hello.txt" // TODO: 更改路径
        val inputDS: DataSet[String] = env.readTextFile(inputPath)
        // 分词之后，对单词进行groupby分组，然后用sum进行聚合
        val wordCountDS: AggregateDataSet[(String, Int)] = inputDS
            .flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

        // 打印输出
        wordCountDS.print()
    }

}
