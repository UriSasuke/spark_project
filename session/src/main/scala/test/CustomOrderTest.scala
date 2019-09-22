package test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 自定义排序测试
  * Created by Enzo Cotter on 2019/9/22.
  */
object CustomOrderTest {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CustomOrderTest").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val girl: Array[String] = Array("reba,18,80", "mimi,22,70", "liya,30,80", "jingtian,18,85")
    val girlRDD: RDD[String] = sparkSession.sparkContext.parallelize(girl)

    val rdd = girlRDD.map {
      item =>
        val arr: Array[String] = item.split(",")
        val name = arr(0)
        val age = arr(1).toInt
        val weight = arr(2).toInt
        (name, age, weight)
    }

    val orderGirlRDD = rdd.sortBy(s => new Girl(s._1,s._2,s._3))
    val r = orderGirlRDD.collect()
    println(r.toBuffer)
    sparkSession.stop()
  }

  case class Girl(val name: String, val age: Int, val weight: Int) extends Ordered[Girl] with Serializable {

    override def compare(that: Girl): Int = {

      if (this.age == that.age) {
        that.weight - this.weight
      } else {
        this.age - that.age
      }
    }

    override def toString: String = s"名字：$name,年龄：$age,体重：$weight"
  }

}
