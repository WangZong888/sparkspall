package com.atguigu.sparkmall.mock

import com.atguigu.sparkmall.customerutil.{MyKafkaUtil, RedisUtil}
import com.atguigu.sparkspall.common.util.DateUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods


object Req6DateAreaAdsClickCountTop3Application {

  def main(args: Array[String]): Unit = {

    import redis.clients.jedis.Jedis

    //TODO 需求六：每天各地区 top3 热门广告

    // TODO 准备SparkStreaming上下文环境对象
    val conf: SparkConf = new SparkConf().setAppName("Req5AdsClickCountApplication").setMaster("local[*]")
    val streaming = new StreamingContext(conf, Seconds(5))
    streaming.sparkContext.setCheckpointDir("cp1")
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

    // TODO 1. 将数据转换结构 （date-area-city-ads, 1）
    val dateAreaCityAdsAndOne: DStream[(String, Long)] = kafkaDS.map(message => {
      val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)
    })
    // TODO 2. 将转换后的结构后的数据进行有状态聚合
    val stateDStream: DStream[(String, Long)] = dateAreaCityAdsAndOne.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.size
        Option(sum)
      }
    }
    // TODO 3. 将聚合后的结果进行结构的转换（date-area-ads, sum）,（date-area-ads, sum）
    val dateAreaAdsToSumDStream: DStream[(String, Long)] = stateDStream.map(action => {
      val keys: Array[String] = action._1.split("_")
      (keys(0) + "_" + keys(1) + "_" + keys(3), action._2)
    })
    // TODO 4. 将转换结构后的数据进行聚合（date-area-ads, totalSum）
    val dateAreaAdsToTotalSumDStream: DStream[(String, Long)] = dateAreaAdsToSumDStream.reduceByKey(_ + _)
    // TODO 5. 将聚合后的结果进行结构的转换（date-area-ads, totalSum）==> （date-area, (ads, totalSum)）
    val dateAreaToAdsTotalsumDStream: DStream[(String, (String, Long))] = dateAreaAdsToTotalSumDStream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0) + "_" + keys(1), (keys(2), sum))
      }
    }
    // TODO 6. 将数据进行分组
    val groupDStream: DStream[(String, Iterable[(String, Long)])] = dateAreaToAdsTotalsumDStream.groupByKey()
    // TODO 7. 对分组后的数据排序（降序），取前三
    val resultDStream: DStream[(String, Map[String, Long])] = groupDStream.mapValues(datas => {
      datas.toList.sortWith(_._2 > _._2).take(3).toMap
    })
    // TODO 8. 将结果保存到redis中
    resultDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(action =>{
        val client: Jedis = RedisUtil.getJedisClient
        action.foreach{
          case (key ,map)=>{
            val keys: Array[String] = key.split("_")
            val k = "top3_ads_per_day:"+keys(0)
            val f = keys(1)
            import org.json4s.JsonDSL._
            val v = JsonMethods.compact(JsonMethods.render(map))
            client.hset(k,f,v)
          }
        }
        client.close()
      })
    })
    //记得一定要开启,启动采集器
    streaming.start()
    //一定要等待,Driver应该等待采集器的执行结束
    streaming.awaitTermination()
  }

}
