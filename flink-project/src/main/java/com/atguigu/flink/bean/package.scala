package com.atguigu.flink

package object bean {

  //TODO  可以声明称普通的类，但是普通的类有限制，
  //TODO  1.普通类在使用的时候需要new关键字
  //TODO  2.普通类不能使用case模式匹配
  //TODO  3.样例类不需要声明get  set 方法

  case class ReceiptEvent(
                           txId: String,
                           payChannel: String,
                           eventTime: Long )

  case class OrderEvent(
                         orderId: Long,
                         eventType: String,
                         txId:String ,
                         eventTime: Long
                       )
  case class OrderResult(
                          orderId: Long,
                          start: Long,
                          end:Long
                        )

  case class LoginEvent(userId: Long,
                        ip: String,
                        eventType: String,
                        eventTime: Long)

  case class AdClickLog(
                         userId: Long,
                         adId: Long,
                         province: String,
                         city: String,
                         timestamp: Long)

  case class CountByProvince(
                              windowEnd: String,
                              province: String,
                              adId:Long,
                              count: Long)


  case class MarketingUserBehavior(userId: String,
                                   behavior: String,
                                   channel: String,
                                   timestamp: Long
                                  )

  /**
    * 请求日志样例类
    * @param ip
    * @param userId
    * @param eventTime
    * @param method
    * @param usl
    */
  case  class ResourceLog(
                             ip: String,
                             userId: String,
                             eventTime: Long,
                             method: String,
                             url: String
                           )

  /**
    * 页面点击汇总
    * @param method
    * @param clickCount
    * @param windowEndTime
    */
  case  class ResourceClick(
                             url:String,
                             clickCount: Long,
                             windowEndTime:Long
                           )


  /**
    * 用户行为数据
    * @param userId
    * @param itemId
    * @param categoryId
    * @param behavior
    * @param timestamp
    */
  case  class UserItemClick(
                             userId: Long,
                             itemId: Long,
                             categoryId: Long,
                             behavior: String,
                             timestamp: Long
                           )

  case  class ClickCount(
                       itemId:Long,
                       clickCount: Long,
                       windowEndTime:Long
                       )
}
