package com.tian.flink.env

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author tian
 * @date 2019/10/20 11:43
 * @version 1.0.0
 */
class GetEnvironments {
    def main(args: Array[String]): Unit = {
        //会根据查询运行的方式决定返回什么样的运行环境
        val env1: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //返回本地执行环境，需要在调用时指定默认的并行度
        val env2: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
        //返回集群执行环境，将Jar提交到远程服务器，需要在调用时指定JobManager的IP和端口号，并指定要在集群中运行的Jar包
        val env3: ExecutionEnvironment =
            ExecutionEnvironment.createRemoteEnvironment("localhost",6123,"/opt/jars/wordcount.jar")
    }
}
