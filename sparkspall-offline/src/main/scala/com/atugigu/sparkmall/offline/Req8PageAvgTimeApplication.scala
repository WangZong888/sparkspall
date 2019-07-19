package com.atugigu.sparkmall.offline

import com.atguigu.sparkspall.common.model.UserVisitAction
import com.atguigu.sparkspall.common.util.{ConfigurationUtil, DateUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Req8PageAvgTimeApplication {
  def main(args: Array[String]): Unit = {


    // TODO 需求八 ：页面平均停留时间

    // TODO 4.0 创建SparkSQL的环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //设置检查点的路劲
    spark.sparkContext.setCheckpointDir("cp")
    //隐式类
    import spark.implicits._
    // TODO 4.1 从Hive中获取满足条件的数据
    spark.sql("use " + ConfigurationUtil.getValueByKey("hive.database"))
    var sql = "select * from user_visit_action where 1=1"
    //获取条件
    val startDate: String = ConfigurationUtil.getValueByJsonKey("startDate")
    val endDate: String = ConfigurationUtil.getValueByJsonKey("endData")
    if (StringUtil.isNotEmply(startDate)) {
      sql = sql + " and date >= '" + startDate + "'"
    }
    if (StringUtil.isNotEmply(endDate)) {
      sql = sql + " and date <='" + endDate + "'"
    }

    val actionDF: DataFrame = spark.sql(sql)
    //添加类型
    val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
    //使用RDD来操作
    val actionRDD: RDD[UserVisitAction] = actionDS.rdd
    //重复使用的数据使用checkpoint来进行保存起来-只有在计算数据的时候才保存
    actionRDD.checkpoint()

    //TODO******************************** 需求八的代码 *********************************
    // TODO 1. 将数据根据session进行分组
    val groupBySession: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    // TODO 2. 将分组后的数据进行时间排序(升序)
    val sessionToPageidAndTimeXRDD: RDD[(String, List[(Long, Long)])] = groupBySession.mapValues(action => {
      val sortlist: List[UserVisitAction] = action.toList.sortWith(_.action_time < _.action_time)
      // TODO 3. 将页面数据进行拉链((1-2),(time2-time1))-进行数据转换
      val idToTimeList: List[(Long, String)] = sortlist.map(action => {
        (action.page_id, action.action_time)
      })
      //进行拉链 ( (pageid1, time1), (pageid2, time2)  )
      val pageid1ToPageid2List: List[((Long, String), (Long, String))] = idToTimeList.zip(idToTimeList.tail)
      // TODO 4. 将拉链数据进行结构的转变(1,(timeX)),(1,(timeX)),(1,(timeX))-最终想要的格式是（pageid,timexs）
      pageid1ToPageid2List.map {
        case (page1, page2) => {
          val time1: Long = DateUtil.parseLongByString(page1._2)
          val time2: Long = DateUtil.parseLongByString(page2._2)
          (page1._1, time2 - time1)
        }
      }
    })
    // TODO 5. 将转变结构后的数据进行分组(pageid, Iterator[(time)])
    val pageidToTimeXRDD: RDD[List[(Long, Long)]] = sessionToPageidAndTimeXRDD.map(_._2)
    //扁平化
    val flatMapRDD: RDD[(Long, Long)] = pageidToTimeXRDD.flatMap(list => list)
    //分组聚合,将相同页面的停留时间聚合在一起了
    val groupByKeyRDD: RDD[(Long, Iterable[Long])] = flatMapRDD.groupByKey()
    // TODO 6. 获取最终结果：(pageid, timeSum / timeSize])
    groupByKeyRDD.foreach{
      case (pageid ,timexs) =>{
        println("在 "+pageid+" 页面停留了 "+ (timexs.sum / timexs.size))
      }
    }
    //TODO******************************* 需求八的代码 *********************************
    //println(top10List)
    // TODO 4.7 释放资源
    spark.stop()
  }

}


