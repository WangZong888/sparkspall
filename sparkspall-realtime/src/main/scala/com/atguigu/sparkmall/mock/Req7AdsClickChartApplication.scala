package com.atguigu.sparkmall.mock

import com.atguigu.sparkmall.customerutil.{MyKafkaUtil, RedisUtil}
import com.atguigu.sparkspall.common.util.DateUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods


object Req7AdsClickChartApplication {

  def main(args: Array[String]): Unit = {


    // TODO 需求七：最近一分钟广告点击趋势（每10秒）

    // TODO 准备SparkStreaming上下文环境对象
    val conf: SparkConf = new SparkConf().setAppName("Req5AdsClickCountApplication").setMaster("local[*]")
    val streaming = new StreamingContext(conf, Seconds(5))

    val topic = "ads_log0218"

    // TODO 从Kafka中获取数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streaming)

    //TODO 将获取的kafka数据转换结构
    val kafkaDS: DStream[Mykafka] = kafkaStream.map {
      action => {
        val values: Array[String] = action.value().split(" ")
        Mykafka(values(0), values(1), values(2), values(3), values(4))
      }
    }

    // TODO 1. 使用窗口函数将数据进行封装
    val windowDStream: DStream[Mykafka] = kafkaDS.window(Seconds(60),Seconds(20))
    // TODO 2. 将数据进行结构的转换 （ 15：11 ==> 15:10 , 15:25 ==> 15:20 ）
    val timeToOneDStream: DStream[(String, Long)] = windowDStream.map(data => {
      val date: String = DateUtil.formatStringByTimestamp(data.timestamp.toLong)
      val time: String = date.substring(0, date.length - 1) + "0"
      (time, 1L)
    })
    // TODO 3. 将转换结构后的数据进行聚合统计
    val timeToSumDStream: DStream[(String, Long)] = timeToOneDStream.reduceByKey(_+_)
    // TODO 4. 对统计结果进行排序
    val sortDStream: DStream[(String, Long)] = timeToSumDStream.transform(_.sortByKey())

    sortDStream.print()
    //记得一定要开启,启动采集器
    streaming.start()
    //一定要等待,Driver应该等待采集器的执行结束
    streaming.awaitTermination()
  }

}
