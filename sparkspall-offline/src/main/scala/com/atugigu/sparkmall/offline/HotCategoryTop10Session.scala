package com.atugigu.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.sparkspall.common.model.UserVisitAction
import com.atguigu.sparkspall.common.util.{ConfigurationUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


import scala.collection.{immutable, mutable}


object HotCategoryTop10Session {
  def main(args: Array[String]): Unit = {

    // 需求一 ： Top10 热门品类中 Top10 活跃 Session 统计
    // TODO 4.0 创建SparkSQL的环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
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
    // TODO 4.2 使用累加器累加数据，进行数据的聚合( categoryid-click,100  ), ( categoryid-order,100  ), ( categoryid-pay,100  )
    val acc = new MyAccumulator
    //注册
    spark.sparkContext.register(acc, "accRes")
    //执行accumulator
    actionRDD.foreach {
      actionData => {
        if (actionData.click_category_id != -1) {
          acc.add(actionData.click_category_id + "-click")
        } else if (StringUtil.isNotEmply(actionData.order_category_ids)) {
          val ids: Array[String] = actionData.order_category_ids.split(",")
          for (id <- ids) {
            acc.add(id + "-order")
          }
        } else if (StringUtil.isNotEmply(actionData.pay_category_ids)) {
          val ids: Array[String] = actionData.pay_category_ids.split(",")
          for (id <- ids) {
            acc.add(id + "-pay")
          }
        }
      }
    }

    //拿到value
    // (category-指标，sumcount)
    val accData: mutable.HashMap[String, Long] = acc.value
    // TODO 4.3 将累加器的结果通过品类ID进行分组（ categoryid，[（order,100）,(click:100), (pay:100)] ）
    val categoryGroup: Map[String, mutable.HashMap[String, Long]] = accData.groupBy {
      case (key, sumcount) => {
        val keys: Array[String] = key.split("-")
        keys(0)
      }
    }
    // TODO 4.4 将分组后的结果转换为对象CategoryTop10(categoryid, click,order,pay )
    val taskId: String = UUID.randomUUID().toString
    val categoryTop10Datas: immutable.Iterable[CategoryTop10] = categoryGroup.map {
      case (categoryId, map) => {
        CategoryTop10(
          taskId,
          categoryId,
          map.getOrElse(categoryId + "-click", 0L),
          map.getOrElse(categoryId + "-order", 0L),
          map.getOrElse(categoryId + "-pay", 0L)
        )
      }
    }
    // TODO 4.5 将转换后的对象进行排序（点击，下单， 支付）（降序）
    val sortList: List[CategoryTop10] = categoryTop10Datas.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }
    // TODO 4.6 将排序后的结果取前10名
    val top10List: List[CategoryTop10] = sortList.take(10)
    val ids: List[String] = top10List.map(_.categoryId)

    //******************************** 需求二的代码 *********************************
    // TODO 4.1 将数据进行过滤筛选，留下满足条件数据（点击数据，品类前10）
    val idsBroad: Broadcast[List[String]] = spark.sparkContext.broadcast(ids)
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter {
      action => {
        if (action.click_category_id != -1) {
          idsBroad.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    }
    // TODO 4.2 将过滤后的数据进行结构的转换（ categoryId+sessionId, 1 ）
    val categoryIdAndSessionToOneRDD: RDD[(String, Int)] = filterRDD.map {
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    }
    // TODO 4.3 将转换结构后的数据进行聚合统计（ categoryId+sessionId, sum ）
    val categoryIdAndSessionToSum: RDD[(String, Int)] = categoryIdAndSessionToOneRDD.reduceByKey(_ + _)
    // TODO 4.4 将聚合后的结果数据进行结构的转换:
    //         （ categoryId+sessionId, sum ）（ categoryId, (sessionId, sum ）)
    val categoryIdToSessionAndSum: RDD[(String, (String, Int))] = categoryIdAndSessionToSum.map {
      action => {
        (action._1.split("_")(0), (action._1.split("_")(1), action._2))
      }
    }
    // TODO 4.5 将转换结构后的数据进行分组：（categoryId， Iterator[(sessionId, sum)]）
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = categoryIdToSessionAndSum.groupByKey()
    // TODO 4.6 将分组后的数据进行排序取前10
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(_.toList.sortWith(_._2 > _._2).take(10))

    //将分组后的结果转换成CategorySessionTop10对象
    val mapRDD: RDD[List[CategorySessionTop10]] = resultRDD.map {
      case (categoryId, list) => {
        list.map {
          case (sessionId, sum) => {
            CategorySessionTop10(taskId, categoryId, sessionId, sum)
          }
        }
      }
    }
    //将List展开
    val dataRDD: RDD[CategorySessionTop10] = mapRDD.flatMap(list => list)
    // TODO 4.7 将结果保存到Mysql中

    dataRDD.foreachPartition {
      datas => {
        val driverClass = ConfigurationUtil.getValueByKey("jdbc.driver.class")
        val url = ConfigurationUtil.getValueByKey("jdbc.url")
        val user = ConfigurationUtil.getValueByKey("jdbc.user")
        val password = ConfigurationUtil.getValueByKey("jdbc.password")
        Class.forName(driverClass)
        val connection: Connection = DriverManager.getConnection(url, user, password)
        val insertSQL = "insert into category_top10_session_count ( taskId, categoryId, sessionId, clickCount ) values ( ?, ?, ?, ? )"
        val pdst: PreparedStatement = connection.prepareStatement(insertSQL)
        datas.foreach {
          data => {
            pdst.setString(1, data.taskId)
            pdst.setString(2, data.categoryId)
            pdst.setString(3,data.sessionId)
            pdst.setLong(4, data.clickCount)

            pdst.executeUpdate()
          }
        }
        //释放资源
        pdst.close()
        connection.close()
      }
    }
    //******************************** 需求二的代码 *********************************
    //println(top10List)
    // TODO 4.7 释放资源
    spark.stop()
  }

}

//样例类
case class CategorySessionTop10(taskId: String, categoryId: String,sessionId: String, clickCount: Long)

