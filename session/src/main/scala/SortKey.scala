/**
  *
  * 自定义排序法：根据clickCount，orderCount，payCount排序
  * Created by Enzo Cotter on 2019/9/20.
  */
case class SortKey(val clickCount: Long, val orderCount: Long, val payCount: Long) extends Ordered[SortKey] with Serializable {

  override def compare(that: SortKey): Int = {
    if (this.clickCount != that.clickCount) {
      (this.clickCount - that.clickCount).toInt
    } else if (this.orderCount != that.orderCount) {
      (this.orderCount - that.orderCount).toInt
    } else {
      (this.payCount - that.payCount).toInt
    }
  }
}
