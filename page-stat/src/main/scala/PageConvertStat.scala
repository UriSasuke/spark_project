import java.util.UUID

import common.conf.ConfigurationManager
import common.constant.Constants
import common.model.{PageSplitConvertRate, UserVisitAction}
import common.utils.DateUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable



/**
  *
  * 需求五：页面单跳转化率统计
  * Created by Enzo Cotter on 2019/9/22.
  */
object PageConvertStat {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("PageConvertStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //参数json
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val paramJson = JSONObject.fromObject(jsonStr)

    val targetPageFlow = paramJson.get(Constants.PARAM_TARGET_PAGE_FLOW).toString
    val targetPageFlowList = targetPageFlow.split(",").map(item => item)

    //获取目标页面切片
    val targetZipList = targetPageFlowList
      .slice(0, targetPageFlowList.length).zip(targetPageFlowList.tail)
      .map {
        case (page1, page2) =>
          page1 + "_" + page2
      }

    //获取UserVisition数据
    val userVisitActionRDD = getUserVisitActionRDD(sparkSession, paramJson)
    val sessionId2UserVisitActionRDD = userVisitActionRDD.map {
      item => (item.session_id, item)
    }
    val sessionId2GroupRDD = sessionId2UserVisitActionRDD.groupByKey()

    //    sessionId2GroupRDD.foreach(println(_))

    val pageSplitMapRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        //对action进行排序，按时间正序
        val sortList = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })
        //获取pageId集合
        val pageIdList = sortList.map {
          case item => item.page_id
        }

        val pageList = pageIdList.slice(0, pageIdList.size - 1).zip(pageIdList.tail).map {
          case (page1, page2) =>
            page1 + "_" + page2
        }

        val pageSplitFilter = pageList.filter(
          pageSplit => targetZipList.contains(pageSplit)
        )

        val pageSplitMap = pageSplitFilter.map {
          case pageSplit =>
            (pageSplit, 1L)
        }

        pageSplitMap
    }

    //Map[String,Long]
    val prgeSplitCountMap = pageSplitMapRDD.countByKey()


    val startPage = targetPageFlowList(0).toLong

    //计算第一步的数量
    val startPageCount = sessionId2UserVisitActionRDD.filter {
      case (sessionId, action) => action.page_id == startPage
    }.count()

    val taskUUID = UUID.randomUUID().toString
    getPageConvert(sparkSession, taskUUID, targetZipList, startPageCount, prgeSplitCountMap);
    sparkSession.stop()
  }

  /**
    * 获取单页跳转率
    *
    * @param sparkSession
    * @param taskUUID
    * @param targetZipList
    * @param startPageCount
    * @param prgeSplitCountMap
    */
  def getPageConvert(sparkSession: SparkSession, taskUUID: String,
                     targetZipList: Array[String], startPageCount: Long,
                     prgeSplitCountMap: collection.Map[String, Long]) = {

    val pageCountRadio = new mutable.HashMap[String, Double]()
    var lastPageCount = startPageCount.toDouble
    for (currentPage <- targetZipList) {
      val currentPageCount = prgeSplitCountMap(currentPage).toDouble
      val radio = currentPageCount / lastPageCount
      pageCountRadio.put(currentPage, radio)
      lastPageCount = currentPageCount
    }

    val converRatioStr = pageCountRadio.map {
      case (pageSplit, ratio) =>
        pageSplit + "=" + ratio
    }.mkString("|")

    val pageSplitConvertRate = new PageSplitConvertRate(taskUUID, converRatioStr)

    val pageSplitRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSplitConvertRate))

    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate_0308")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }


  /**
    * 获取符合条件的用户行为数据
    *
    * @param sparkSession
    * @param paramJson
    */
  def getUserVisitActionRDD(sparkSession: SparkSession, paramJson: JSONObject) = {

    val start_date = paramJson.getString(Constants.PARAM_START_DATE)
    val end_date = paramJson.getString(Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date > '" + start_date +
      "' and date < '" + end_date + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }


}
