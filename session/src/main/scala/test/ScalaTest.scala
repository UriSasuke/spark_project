package test

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Enzo Cotter on 2019/9/15.
  */
object ScalaTest {


  def main(args: Array[String]): Unit = {

    val map = new mutable.HashMap[String,Int]()

    map("a") = 2
    map("b") = 2
    map("c") = 3
    map.foreach(println(_))

    map.get("a") match {
      case None => println("这是空值")
      case Some(1) => println("非空值1")
      case Some(2) => map("a") += 2
        println(map("a").intValue())
    }

    val s = null
    if(s ne null) {
      println("s是空")
    }


    map += ("b" -> 10)

    println("b:" + map("b").intValue())


    val a = ListBuffer[Int]()

    for ( i <- 0 to 10) {

      a.append(i)
    }

    println(a)

  }
}
