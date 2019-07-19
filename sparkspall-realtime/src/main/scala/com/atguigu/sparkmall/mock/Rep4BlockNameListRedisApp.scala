package com.atguigu.sparkmall.mock

import java.util

import com.atguigu.sparkmall.customerutil.{MyKafkaUtil, RedisUtil}
import com.atguigu.sparkspall.common.util.DateUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}



object Rep4BlockNameListRedisApp {

  def main(args: Array[String]): Unit = {

    import redis.clients.jedis.Jedis
//   BinaryJedis
    //连接本地的 Redis 服务
//    val jedis = new Jedis("hadoop102", 6379)
//    //查看服务是否运行，打出pong表示OK
//    System.out.println("connection is OK==========>: " + jedis.ping)
    //TODO 需求四：广告黑名单实时统计
    val conf: SparkConf = new SparkConf().setAppName("Rep4BlockNameListRedisApp").setMaster("local[*]")
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
//   kafkaDS.foreachRDD{
//     data =>{
//       data.foreach(println)
//     }
//   }

    // TODO 0. 对数据进行筛选过滤，黑名单数据不需要
    //Driver
    //问题1 ：会发生空指针异常，是因为序列化规则.其中list是transient（瞬时对象）
   /* val filterDStream: DStream[AdsClickKafkaMessage] = kafkaDS.filter(message => {
      // Executor
      !useridsBroadcast.value.contains(message.userid)
    })*/
    //问题2 ：黑名单数据无法更新，应该周期性获取最新的黑名单
    //Driver(1)
    val filterDStream: DStream[Mykafka] = kafkaDS.transform (
      rdd => {
        //Driver(n)
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val userids: util.Set[String] = jedisClient.smembers("blacklist1")
        jedisClient.close()
        //使用广播变量
        val useridsBroad: Broadcast[util.Set[String]] = streaming.sparkContext.broadcast(userids)
        rdd.filter(message => {
          //Executor(n)
          !useridsBroad.value.contains(message.userid)
        })
      })
    // TODO 1. 将数据转换结构 （date-ads-user, 1）
    val mapDStream: DStream[(String, Long)] = filterDStream.map(
      message => {
        val dt: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong,"yyyy-MM-dd")
        (dt + "_" + message.adid + "_" + message.userid, 1L)
      })

    // TODO 2. 将转换结构后的数据进行聚合 、
    mapDStream.foreachRDD(action => {
      action.foreachPartition(rdd =>{
        val client: Jedis = RedisUtil.getJedisClient
        val key ="data:ads:user:click"
        rdd.foreach{
          case (field,one) => {
            client.hincrBy(key,field,1L)
            // TODO 3. 对聚合后的结果进行阈值的判断
            val sum: Long = client.hget(key,field).toLong
            // TODO 4. 如果超出阈值，将用户拉入黑名单
            if(sum >= 100) {
              val fields: Array[String] = field.split("_")
              client.sadd("blacklist1",fields(2))
            }
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
