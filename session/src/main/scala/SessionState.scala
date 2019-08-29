import java.util.Date

import common.conf.ConfigurationManager
import common.constant.Constants
import common.model.UserVisitAction
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * Created by Enzo Cotter on 2019/8/3.
  */
object SessionState {


  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("SessionSate").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()

    //加载条件查询json字符串
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val param = JSONObject.fromObject(jsonStr)

    //获取userVisitActionRDD信息
    val userVisitActionRDD = getUserVisitActionRDD(sparkSession, param)
//    userVisitActionRDD.foreach(println(_))
    val sessionid2actionRDD = userVisitActionRDD.map(item => (item.session_id, item))
//    session2actionRDD.foreach(println(_))
    val sessionid2GroupActionRDD =  sessionid2actionRDD.groupByKey()
    sessionid2GroupActionRDD.foreach(println(_))




    sparkSession.close
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

}
