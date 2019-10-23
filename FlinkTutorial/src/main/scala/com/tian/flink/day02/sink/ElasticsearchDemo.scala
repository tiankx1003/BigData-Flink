package com.tian.flink.day02.sink

import java.util

import com.tian.flink.day02.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * Elasticsearch Sink
 *
 * @author tian
 * @date 2019/10/23 20:31
 * @version 1.0.0
 */
object ElasticsearchDemo {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val socketSource = env.socketTextStream("localhost", 7777)
        import org.apache.flink.streaming.api.scala._
        val inputDataStream = socketSource.map(data => {
            val splits = data.split(",")
            SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
        })

        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("localhost", 9200))
        new ElasticsearchSink.Builder[SensorReading](
            httpHosts,
            new ElasticsearchSinkFunction[SensorReading] {
                override def process(t: SensorReading,
                                     runtimeContext: RuntimeContext,
                                     requestIndexer: RequestIndexer): Unit = {
                    println(("saving data: " + t))
                    val json = new util.HashMap[String, String]()
                    json.put("data", t.toString)
                    val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
                    requestIndexer.add(indexRequest)
                    println("save successfully")
                }
            }
        )
    }
}
