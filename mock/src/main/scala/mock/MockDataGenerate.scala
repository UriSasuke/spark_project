package mock

import java.util.UUID

import common.constant.Constants
import common.model.{ProductInfo, UserInfo, UserVisitAction}
import common.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by Enzo Cotter on 2019/7/27.
  */
object MockDataGenerate {


  /**
    * UserInfo数据自动生成
    *
    * @return
    */
  private def mockUserInfo(): Array[UserInfo] = {

    val rows = new ArrayBuffer[UserInfo]()
    val sexes = Array("male", "female")
    val random = new Random()

    for (i <- 0 to 100) {
      val user_id = i
      val username = "user_" + i
      val name = "name_" + i
      val age = random.nextInt(60)
      val professional = "professional_" + random.nextInt(100)
      val city = "city_" + random.nextInt(50)
      val sex = sexes(random.nextInt(2))
      rows += UserInfo(user_id, username, name, age, professional, city, sex)
    }
    rows.toArray
  }

  /**
    * ProductInfo数据自动生成
    *
    * @return
    */
  private def mockProductInfo(): Array[ProductInfo] = {
    val rows = new ArrayBuffer[ProductInfo]()
    val random = new Random()
    val productStatus = Array(0, 1)
    for (i <- 0 to 100) {

      val product_id = i
      val product_name = "product_" + i
      val extend_info = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"
      rows += ProductInfo(product_id, product_name, extend_info)
    }
    rows.toArray
  }

  /**
    * UserVisitAction数据自动生成
    *
    * @return
    */
  private def mockUserVisitAction(): Array[UserVisitAction] = {
    val searchKeywords = Array("华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯")

    //yyyy-MM-dd
    val date = DateUtils.getTodayDate()
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()

    //一共10000个用户（session有重复）
    for (i <- 0 to 100) {

      val userid = random.nextInt(100)
      // 每个用户产生100个session
      for (j <- 0 to 10) {
        // 不可变的，全局的，独一无二的128bit长度的标识符，用于标识一个session，体现一次会话产生的sessionId是独一无二的
        val sessionid = UUID.randomUUID().toString().replace("-", "")

        // 在yyyy-MM-dd后面添加一个随机的小时时间（0-23）
        val baseActionTime = date + " " + random.nextInt(23)
        // 每个(userid + sessionid)生成0-100000条用户访问数据
        for (k <- 0 to random.nextInt(100)) {
          val pageid = random.nextInt(10)
          // 在yyyy-MM-dd HH后面添加一个随机的分钟时间和秒时间
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid = random.nextInt(10).toLong

          // 随机确定用户在当前session中的行为
          val action = actions(random.nextInt(4))

          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(10))
            case "click" => clickCategoryId = random.nextInt(100).toLong
              clickProductId = String.valueOf(random.nextInt(100)).toLong
            case "order" => orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userid, sessionid,
            pageid, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityid)

        }
      }

    }
    rows.toArray
  }

  def main(args: Array[String]): Unit = {
    //spark配置
    val sparkConf = new SparkConf().setAppName("Mock Data").setMaster("local[*]")
    //spark sql客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val userInfo = this.mockUserInfo()
    val productInfo = this.mockProductInfo()
    val userVisitAction = this.mockUserVisitAction()

    val userInfoRDD = spark.sparkContext.makeRDD(userInfo)
    val productInfoRDD = spark.sparkContext.makeRDD(productInfo)
    val userVisitActionRDD = spark.sparkContext.makeRDD(userVisitAction)

    import spark.implicits._
    insertHive(spark, Constants.USER_INFO_TABLE, userInfoRDD.toDF)
    insertHive(spark, Constants.PRODUCT_INFO_TABLE, productInfoRDD.toDF())
    insertHive(spark, Constants.USER_VISIT_ACTION_TABLE, userVisitActionRDD.toDF())
    spark.close
  }

  /**
    * 插入数据到hive
    * @param sparkSession
    * @param tableName
    * @param dataRDD
    */
  def insertHive(sparkSession: SparkSession, tableName: String, dataDF: DataFrame) ={
    val sql = "DROP TABLE IF EXISTS " + tableName
    sparkSession.sql(sql)
    dataDF.write.saveAsTable(tableName)
  }

}
