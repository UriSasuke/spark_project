import java.util.UUID

import common.conf.ConfigurationManager
import common.constant.Constants
import common.model.{Top10Category, UserVisitAction}
import common.utils.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  *
  * 需求三：Top10热门品类统计
  *
  * Created by Enzo Cotter on 2019/9/18.
  */
object Top10CategoryIdCount {


  def main(args: Array[String]): Unit = {

    //spark环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10CategoryIdCount")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val userVisitActionRDD = getUserVisitActionRDD(sparkSession)

    //点击计数 (categoryId, count)
    val clickCountRDD: RDD[(Long, Long)] = getClickCount(userVisitActionRDD)
    // 下单计数(categoryId, count)
    val orderCountRDD: RDD[(Long, Long)] = getOrderCount(userVisitActionRDD)
    // 支付计数(categoryId, count)
    val payCountRDD: RDD[(Long, Long)] = getPayCount(userVisitActionRDD)
    // 所有类别(categoryId, categoryId)
    val fullCategoryIdRDD: RDD[(Long, Long)] = getFullCategoryIdRDD(userVisitActionRDD)


    val fullCountRDD = getFullCount(fullCategoryIdRDD, clickCountRDD, orderCountRDD, payCountRDD)

    val top10CategoryRDD = getTop10CategoryRDD(sparkSession, fullCountRDD)

    writerMysql(sparkSession, top10CategoryRDD)
    sparkSession.stop()
  }


  /**
    * 写入数据库
    *
    * @param sparkSession
    * @param top10CategoryRDD
    */
  def writerMysql(sparkSession: SparkSession, top10CategoryRDD: RDD[Top10Category]): Unit = {

    import sparkSession.implicits._
    top10CategoryRDD.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category_0308")
      .mode(SaveMode.Append).save
  }


  /**
    * 获取前10的click，order，pay分类
    *
    * @param fullCount
    */
  def getTop10CategoryRDD(sparkSession: SparkSession, fullCount: RDD[(Long, String)]) = {

    val sortKeyRDD = fullCount.map {
      case (categoryId, fullCountInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = new SortKey(clickCount, orderCount, payCount)
        (sortKey, fullCountInfo)
    }
    val top10Data = sortKeyRDD.sortByKey(false).take(10)

    println("==========================================")
    top10Data.foreach(println(_))
    println("==========================================")


    val taskId = UUID.randomUUID().toString

    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Data).map {
      case (sortKey, fullCountInfo) =>
        val categoryId = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount
        new Top10Category(taskId, categoryId, clickCount, orderCount, payCount)
    }
    top10CategoryRDD
  }


  /**
    * 获取全部计数
    * ps：利用fullCategoryIdRDD和其他计数RDD进行leftOuterJoin操作，拼接成字符串字段
    *
    * @param fullCategoryIdRDD 所有类别RDD
    * @param clickCountRDD     点击计数RDD
    * @param orderCountRDD     下单计数RDD
    * @param payCountRDD       支付计数RDD
    */
  def getFullCount(fullCategoryIdRDD: RDD[(Long, Long)],
                   clickCountRDD: RDD[(Long, Long)],
                   orderCountRDD: RDD[(Long, Long)],
                   payCountRDD: RDD[(Long, Long)]) = {

    //点击类别拼装
    val clickInfoRDD = fullCategoryIdRDD.leftOuterJoin(clickCountRDD).map {
      case (categoryId, (categoryId1, option)) =>
        val count = if (option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + count
        (categoryId, aggrCount)
    }

    //下单类别拼装
    val orderInfoRDD = clickInfoRDD.leftOuterJoin(orderCountRDD).map {
      case (categoryId, (clickInfo, option)) =>
        val count = if (option.isDefined) option.get else 0
        val aggrCount = clickInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + count
        (categoryId, aggrCount)
    }

    val payInfoRDD = orderInfoRDD.leftOuterJoin(payCountRDD).map {
      case (categoryId, (orderInfo, option)) =>
        val count = if (option.isDefined) option.get else 0
        val aggrCount = orderInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + count
        (categoryId, aggrCount)
    }

    payInfoRDD
  }


  /**
    * 获取全部的categoryId的RDD 格式 RDD[(Long,Long)]
    *
    * @param userVisitActionRDD
    */
  def getFullCategoryIdRDD(userVisitActionRDD: RDD[UserVisitAction]) = {

    val fuucateGoryIdRDD = userVisitActionRDD.flatMap {
      item =>
        val listBuffer = new ListBuffer[(Long, Long)]
        //获取点击类别ID
        val clickCategoryId: Long = item.click_category_id
        if (item.click_category_id != -1)
          listBuffer += ((clickCategoryId, clickCategoryId))

        //获取下单类别ID
        val orderCategoryIds: String = item.order_category_ids
        if (StringUtils.isNotEmpty(orderCategoryIds))
          orderCategoryIds.split(",").map(item => listBuffer += ((item.toLong, item.toLong)))

        //获取支付类别ID
        val payCategoryIds: String = item.pay_category_ids
        if (StringUtils.isNotEmpty(payCategoryIds)) {
          payCategoryIds.split(",").map(item => listBuffer += ((item.toLong, item.toLong)))
        }
        listBuffer
    }

    //类别去重
    fuucateGoryIdRDD.distinct
  }


  /**
    * 获取点击类别的数量
    *
    * @param userVisitActionRDD
    * @return
    */
  def getClickCount(userVisitActionRDD: RDD[UserVisitAction]) = {

    val filterRDD = userVisitActionRDD.filter(item => item.click_category_id != -1L)
    val countRDD = filterRDD.map(item => (item.click_category_id, 1L))
    countRDD.reduceByKey(_ + _)
  }

  /**
    * 获取下单类别的数量
    *
    * @param userVisitAction
    */
  def getOrderCount(userVisitActionRDD: RDD[UserVisitAction]) = {
    val filterRDD = userVisitActionRDD.filter(item => StringUtils.isNotEmpty(item.order_category_ids))
    val countRDD = filterRDD.flatMap {
      item =>
        item.order_category_ids.split(",")
          .map(item => (item.toLong, 1L))
    }
    countRDD.reduceByKey(_ + _)
  }

  /**
    * 获取支付类别的数量
    *
    * @param userVisitActionRDD
    */
  def getPayCount(userVisitActionRDD: RDD[UserVisitAction]) = {

    val filterRDD = userVisitActionRDD.filter(item => StringUtils.isNotEmpty(item.pay_category_ids))
    val countRDD = filterRDD.flatMap {
      item => item.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    countRDD.reduceByKey(_ + _)
  }


  /**
    * 获取USerVisitAction数据RDD
    *
    * @param sparkSession
    * @return
    */
  def getUserVisitActionRDD(sparkSession: SparkSession): RDD[UserVisitAction] = {
    val sql = "select * from user_visit_action"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

}
