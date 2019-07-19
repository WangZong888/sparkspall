package com.atugigu.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.sparkspall.common.model.UserVisitAction
import com.atguigu.sparkspall.common.util.{ConfigurationUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}
import scala.collection.mutable.HashMap

object HotCategoryTop10 {
  def main(args: Array[String]): Unit = {

    // 需求一 ： 获取点击、下单和支付数量排名前 10 的品类
    // TODO 4.0 创建SparkSQL的环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //隐式类
    import spark.implicits._
    // TODO 4.1 从Hive中获取满足条件的数据
    spark.sql("use "+ConfigurationUtil.getValueByKey("hive.database"))
    var sql = "select * from user_visit_action where 1=1"
    //获取条件
    val startDate: String = ConfigurationUtil.getValueByJsonKey("startDate")
    val endDate: String = ConfigurationUtil.getValueByJsonKey("endData")
    if(StringUtil.isNotEmply(startDate)){
      sql =sql + " and date >= '"+startDate+"'"
    }
    if(StringUtil.isNotEmply(endDate)) {
      sql = sql + " and date <='"+endDate+"'"
    }

    val actionDF: DataFrame = spark.sql(sql)
    //添加类型
    val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
    //使用RDD来操作
    val actionRDD: RDD[UserVisitAction] = actionDS.rdd

    // TODO 4.2 使用累加器累加数据，进行数据的聚合( categoryid-click,100  ), ( categoryid -order,100  ), ( categoryid-pay,100  )
    val acc = new MyAccumulator
    //注册
    spark.sparkContext.register(acc,"accRes")
    //执行accumulator
    actionRDD.foreach{
      actionData =>{
        if(actionData.click_category_id != -1){
          acc.add(actionData.click_category_id+"-click")
        }else if(StringUtil.isNotEmply(actionData.order_category_ids)) {
          val ids: Array[String] = actionData.order_category_ids.split(",")
          for(id <- ids) {
            acc.add(id+"-order")
          }
        }else if(StringUtil.isNotEmply(actionData.pay_category_ids)){
          val ids: Array[String] = actionData.pay_category_ids.split(",")
          for (id <- ids) {
            acc.add(id+"-pay")
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
    // TODO 4.6 将排序后的结果取前10名保存到Mysql数据库中
    val top10List: List[CategoryTop10] = sortList.take(10)

    //将数据保存到数据库中
    val driverClass = ConfigurationUtil.getValueByKey("jdbc.driver.class")
    val url = ConfigurationUtil.getValueByKey("jdbc.url")
    val user = ConfigurationUtil.getValueByKey("jdbc.user")
    val password = ConfigurationUtil.getValueByKey("jdbc.password")
    Class.forName(driverClass)
    val connection: Connection = DriverManager.getConnection(url,user,password)
    val insertSQL = "insert into category_top10 ( taskId, category_id, click_count, order_count, pay_count ) values ( ?, ?, ?, ?, ? )"
    val pdst: PreparedStatement = connection.prepareStatement(insertSQL)
    top10List.foreach{
      data =>{
        pdst.setString(1,data.taskId)
        pdst.setString(2,data.categoryId)
        pdst.setLong(3,data.clickCount)
        pdst.setLong(4,data.orderCount)
        pdst.setLong(5,data.payCount)
        pdst.executeUpdate()
      }
    }
    //释放资源
    pdst.close()
    connection.close()
      //println(top10List)
    // TODO 4.7 释放资源
    spark.stop()
  }

}
//样例类
case class CategoryTop10(taskId:String, categoryId:String, clickCount:Long, orderCount:Long, payCount:Long)
//自定义累加器
// ( categoryid-click,100  ), ( categoryid-order,100  ), ( categoryid-pay,100  )
class MyAccumulator extends AccumulatorV2[String,HashMap[String,Long]]{

  private var map = new HashMap[String, Long]
  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new MyAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(key: String): Unit = {
    map(key) = map.getOrElse(key,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //俩个map的合并
    val map1 = map
    val map2 = other.value
    map = map1.foldLeft(map2){
      (innermap,kv) =>{
        var k = kv._1
        var v = kv._2
        innermap(k) = innermap.getOrElse(k,0L)+v
        innermap
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = {
    map
  }
}
