package test

import common.conf.ConfigurationManager
import common.constant.Constants
import common.model.Top10Category
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Enzo Cotter on 2019/9/22.
  */
object ReadMysqlTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ReadMysql").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    import sparkSession.implicits._

    val top10CategoryRDD = sparkSession.sqlContext.read.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category_0308")
      .load().orderBy("taskId").limit(10).as[Top10Category].rdd
    top10CategoryRDD.foreach(println(_))
    sparkSession.stop()
  }

}
