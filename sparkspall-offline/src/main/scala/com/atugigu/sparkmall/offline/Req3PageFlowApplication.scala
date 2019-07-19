package com.atugigu.sparkmall.offline



import com.atguigu.sparkspall.common.model.UserVisitAction
import com.atguigu.sparkspall.common.util.{ConfigurationUtil, StringUtil}
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}




object Req3PageFlowApplication {
  def main(args: Array[String]): Unit = {

    // 需求三 ： 页面单跳转化率统计
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

    //TODO******************************** 需求三的代码 *********************************
    // TODO 4.1 计算分母的数据

    // TODO 4.1.1 将原始数据进行就结构的转换，用于统计分析（ pageid, 1 ）
    // 拿到我们需要的过滤条件
    val pageIds: Array[String] = ConfigurationUtil.getValueByJsonKey("targetPageFlow").split(",")
    //进行拉链scala的zip（1,2），（2,3），在进行转换（1-2）
    val pageToFlow: Array[String] = pageIds.zip(pageIds.tail).map {
      case (pageids1, pageids2) => {
        (pageids1 + "-" + pageids2)
      }
    }
    //过滤数据
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter {
      action => {
        pageIds.contains(action.page_id.toString)
      }
    }
    //转换结构（ pageid, 1 ）
    val mapRDD: RDD[(Long, Long)] = filterRDD.map(action => (action.page_id, 1L))
    // TODO 4.1.2 将转换后的结果进行聚合统计（ pageid, 1 ） （pageid, sumcount）
    val pageIdToSumRDD: RDD[(Long, Long)] = mapRDD.reduceByKey(_ + _)
    //生成数据，使用action算子,将数组类型中的tuple类型转换成map类型
    val pageIdToSumMap: Map[Long, Long] = pageIdToSumRDD.collect().toMap
    // TODO 4.2 计算分子的数据
    // TODO 4.2.1 将原始数据通过session进行分组（ sessionId, iterator[ (pageid, action_time) ] ）
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    // TODO 4.2.2 将分组后的数据使用时间进行排序（升序）
    val sessionToPageFlowRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues { action => {
      val datas: List[UserVisitAction] = action.toList.sortWith(_.action_time < _.action_time)
      //      //取出pageId
      //1,2,3,4,5,6,7.....
      val sessionPageIds: List[Long] = datas.map(_.page_id)
      // (1-2,1), (2-3,1), 3-4, 4-5, 5-6, 6-7
      // TODO 4.2.3 将排序后的数据进行拉链处理，形成单跳页面流转顺序
      sessionPageIds.zip(sessionPageIds.tail).map {
        case (pageId1, pageId2) => {
          // TODO 4.2.4 将处理后的数据进行筛选过滤，保留需要关心的流转数据（session, pageid1-pageid2, 1）
          (pageId1 + "-" + pageId2, 1)
        }
      }
    }
    }
    // (1-2,1),(1-9,1),(2-8,1)
    // TODO 4.2.5 对过滤后的数据进行结构的转化（pageid1-pageid2, 1）, 去掉session
    val mapRDD1: RDD[List[(String, Int)]] = sessionToPageFlowRDD.map(_._2)
    //偏平化
    val flatMapRDD1: RDD[(String, Int)] = mapRDD1.flatMap(list => list)
    //过滤数据，只要1-2,2-3,3-4,4-5,5-6,6-7
    val finalPageFlowRDD: RDD[(String, Int)] = flatMapRDD1.filter {
      case (pageFlow, one) => {
        pageToFlow.contains(pageFlow)
      }
    }
    // TODO 4.2.6 将转换结构后的数据进行聚合统计（pageid1-pageid2, sumcount1）
    val resultRDD: RDD[(String, Int)] = finalPageFlowRDD.reduceByKey(_+_)

    // TODO 4.3 使用分子数据除以分母数据：（sumcount1 / sumcount）
    resultRDD.foreach{
      action => {
        val pageids: Array[String] = action._1.split("-")
        val sum: Double = action._2.toDouble / pageIdToSumMap.getOrElse(pageids(0).toLong,1L)
        println(action._1+" = "+sum)
      }
    }

    //TODO******************************* 需求三的代码 *********************************
    //println(top10List)
    // TODO 4.7 释放资源
    spark.stop()
  }

}


