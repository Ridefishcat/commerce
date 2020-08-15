case class SortKey(clickCount:Long,orderCount:Long,payCount:Long) extends Ordered[SortKey] {

  /**
   * 重写compare方法
   * 先比较点击数，如果点击数相同比较订单数，如果订单数相同，比较付款数
   * @param that
   * @return
   */

  // this.compare(that)
  // this compare that
  // compare > 0   this > that
  // compare <0    this < that

  override def compare(that: SortKey): Int = {
    if(this.clickCount - that.clickCount !=0){
      return (this.clickCount-that.clickCount).toInt
    }else if(this.orderCount - that.orderCount != 0){
      return (this.orderCount-that.orderCount).toInt
    }else{
      return (this.payCount-that.payCount).toInt
    }
  }
}
