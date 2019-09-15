import java.util.{Date, UUID}

import common.conf.ConfigurationManager
import common.constant.Constants
import common.model.{SessionAggrStat, UserInfo, UserVisitAction}
import common.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable


/**
  * Created by Enzo Cotter on 2019/8/3.
  */
object SessionState {


  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("SessionSate").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()

    // 创建全局唯一的主键
    val taskUUID = UUID.randomUUID().toString

    //加载条件查询json字符串
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val param = JSONObject.fromObject(jsonStr)

    //获取userVisitActionRDD信息
    val userVisitActionRDD = getUserVisitActionRDD(sparkSession, param)

    //以session_id为key，以对象为value转成map
    val sessionid2actionRDD = userVisitActionRDD.map(item => (item.session_id, item))
    ////    session2actionRDD.foreach(println(_))
    val sessionid2GroupActionRDD = sessionid2actionRDD.groupByKey()

    //获取全部的用户信息
    val sessionId2FullInfoRDD = getSessionActionFullInfo(sparkSession, sessionid2GroupActionRDD)

    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)

    val session2FilteredRDD = getSessionFilteredRDD(param, sessionId2FullInfoRDD, sessionAccumulator)
    session2FilteredRDD.foreach(print(_))


    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)

    sparkSession.close
  }


  /**
    * 写入到数据库
    *
    * @param sparkSession
    * @param taskUUID
    * @param value
    */
  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]) = {

    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)


    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))
    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_0416")
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 写入到数据库
    *
    * @param sparkSession
    * @param taskUUID
    * @param session2FilteredRDD
    */


  /**
    * 统计时长和步长
    *
    * @param taskParam
    * @param sessionId2FullInfoRDD
    * @param sessionAccumulator
    */
  def getSessionFilteredRDD(taskParam: JSONObject,
                            sessionId2FullInfoRDD: RDD[(String, String)],
                            sessionAccumulator: SessionAccumulator): RDD[(String, String)] = {

    //开始年龄
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    //结束年龄
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    //专业
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    //城市
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    //性别
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    //关键字
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    //类目ID
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success = true
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        }
        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }
        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val visitStep = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          //计算时长
          calculateVisitLength(visitLength, sessionAccumulator)
          //计算步长
          calculateStepLength(visitStep, sessionAccumulator)
        }
        success
    }

  }

  /**
    * 计算时长
    *
    * @param visitLength
    * @param sessionAccumulator
    */
  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator): Unit = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)

    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)

    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)

    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)

    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)

    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)

    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)

    } else if (visitLength > 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  /**
    * 计算步长
    *
    * @param stepLength
    * @param sessionAccumulator
    */
  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator): Unit = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)

    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)

    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)

    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)

    } else if (stepLength > 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_60)

    }
  }


  /**
    * 查询hive库中的user_visit_action数据，并转换为RDD
    *
    * @param sparkSession
    * @param param
    * @return
    */
  def getUserVisitActionRDD(sparkSession: SparkSession, param: JSONObject): RDD[UserVisitAction] = {
    val start_date = param.getString(Constants.PARAM_START_DATE)
    val end_date = param.getString(Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date > '" + start_date +
      "' and date < '" + end_date + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }


  /**
    * 获取时长，步长信息
    *
    * @param sparkSession
    * @param sessionid2actionRDD
    * @return
    */
  def getSessionActionFullInfo(sparkSession: SparkSession,
                               sessionid2actionRDD: RDD[(String, Iterable[UserVisitAction])]): RDD[(String, String)] = {

    val userId2AggrInfoRDD = sessionid2actionRDD.map {

      case (sessionId, iterableAction) =>

        var userId = -1L
        var startTime: Date = null
        var endTime: Date = null
        var stepLength = 0
        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        for (action <- iterableAction) {

          if (-1L == userId) {
            userId = action.user_id
          }

          //将开始时间和结束时间替换成实际操作时间
          val actionTime = DateUtils.parseTime(action.action_time)
          if (null == startTime || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (null == endTime || endTime.before(actionTime)) {
            endTime = actionTime
          }


          if (StringUtils.isNotEmpty(action.search_keyword) && !searchKeywords.toString.contains(action.search_keyword)) {
            searchKeywords.append(action.search_keyword).append(",")
          }
          if (-1L != action.click_category_id && !clickCategories.toString.contains(action.click_category_id)) {
            clickCategories.append(action.click_category_id).append(",")
          }

          stepLength += 1

        }

        val searchKey = StringUtils.trimComma(searchKeywords.toString)
        val clickCate = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val stringBuilder = new StringBuilder()

        stringBuilder.append(Constants.FIELD_SESSION_ID).append("=").append(sessionId).append("|")
          .append(Constants.FIELD_SEARCH_KEYWORDS).append("=").append(searchKey).append("|")
          .append(Constants.FIELD_CLICK_CATEGORY_IDS).append("=").append(clickCate).append("|")
          .append(Constants.FIELD_VISIT_LENGTH).append("=").append(visitLength).append("|")
          .append(Constants.FIELD_STEP_LENGTH).append("=").append(stepLength).append("|")
          .append(Constants.FIELD_START_TIME).append("=").append(DateUtils.formatTime(startTime))

        (userId, stringBuilder.toString)
    }

    val sql = "select * from user_info"

    import sparkSession.implicits._
    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2InfoRDD).map {

      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(
          aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
    sessionId2FullInfoRDD
  }

}
