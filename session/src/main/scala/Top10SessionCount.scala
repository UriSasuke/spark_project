import java.util.UUID

import Top10CategoryIdCount.getUserVisitActionRDD
import common.conf.ConfigurationManager
import common.constant.Constants
import common.model.{Top10Category, Top10Session, UserVisitAction}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  *
  * 需求四：Top10热门品类的Top10活跃Session统计
  *
  * Created by Enzo Cotter on 2019/9/22.
  */
object Top10SessionCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Top10SessionCount").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val categoryIdList = getTop10CategoryRDD(sparkSession).map(item => item.categoryId).collect().toList

    //过滤后的包含top前10的所有的session-click
    val sessionClickRDD = getUserVisitActionRDD(sparkSession)
      .map(item => (item.session_id, item.click_category_id)).filter {
      case (k, v) =>
        categoryIdList.contains(v)
    }
    //分组
    val sessionClickGroupRDD = sessionClickRDD.groupByKey()
    val categorySessionRDD = sessionClickGroupRDD.flatMap {
      case (sessionId, iterableCategoryId) =>
        var map = new mutable.HashMap[Long, Long]()
        for (elem <- iterableCategoryId) {
          if (!map.contains(elem)) {
            map += (elem -> 0)
          }
          map.update(elem, map(elem) + 1)
        }
        for ((categoryId, count) <- map)
          yield (categoryId, sessionId + "=" + count)
    }

    val categorySessionGroupRDD = categorySessionRDD.groupByKey()

    categorySessionGroupRDD.foreach(println(_))


    val taskUUID = UUID.randomUUID().toString

   val top10SessionRDD =   categorySessionGroupRDD.flatMap {
      case (categoryId, iterableSessionCount) =>
        val sortList = iterableSessionCount.toList.sortWith((v1, v2) => {
          v1.split("=")(1).toLong > v2.split("=")(1).toLong
        }).take(10)
        val top10Session = sortList.map {
          item =>
            new Top10Session(taskUUID, categoryId, item.split("=")(0), item.split("=")(1).toLong)
        }
        top10Session
    }


    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_0308")
      .mode(SaveMode.Append)
      .save()

    sparkSession.stop()
  }



  /**
    * 获取Top10热门品类的
    *
    * @param sparkSession
    * @return
    */
  def getTop10CategoryRDD(sparkSession: SparkSession) = {

    import sparkSession.implicits._
    val top10CategoryRDD = sparkSession.sqlContext.read.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category_0308")
      .load().orderBy("taskId").limit(10).as[Top10Category].rdd
    top10CategoryRDD
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
