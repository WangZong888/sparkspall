package com.atguigu.sparkmall.mock



import com.atguigu.sparkmall.customerutil.{MyKafkaUtil, RedisUtil}
import com.atguigu.sparkspall.common.util.DateUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf

import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Req5AdsClickCountApplication {

  def main(args: Array[String]): Unit = {

    import redis.clients.jedis.Jedis
//   BinaryJedis
    //连接本地的 Redis 服务
//    val jedis = new Jedis("hadoop102", 6379)
//    //查看服务是否运行，打出pong表示OK
//    System.out.println("connection is OK==========>: " + jedis.ping)
    //TODO 需求五：每天各地区各城市各广告的点击流量实时统计
    val conf: SparkConf = new SparkConf().setAppName("Req5AdsClickCountApplication").setMaster("local[*]")
    val streaming = new StreamingContext(conf,Seconds(5))
    val topic = "ads_log0218"

    // TODO 从Kafka中获取数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streaming)

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
    // TODO 2. 将转换结构后的数据进行聚合 （date-area-city-ads, sum)
    val dateAreaCityAdsAndSum: DStream[(String, Long)] = dateAreaCityAdsAndOne.reduceByKey(_+_)
    // TODO 3. 更新Redis中最终的统计结果-foreachPartition是为了解决所有的连接不能序列化的一种优化方法
    dateAreaCityAdsAndSum.foreachRDD(action =>{
      action.foreachPartition(datas =>{
        val client: Jedis = RedisUtil.getJedisClient
        val key = "date:area:city:ads"
        datas.foreach{
          case (field,sum) =>{
            client.hincrBy(key,field,sum)
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
