/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package common.model

//***************** 输入表 *********************

/**
  * 用户访问动作表
  *
  * @param date               用户点击行为的日期
  * @param user_id            用户的ID
  * @param session_id         Session的ID
  * @param page_id            某个页面的ID
  * @param action_time        点击行为的时间点
  * @param search_keyword     用户搜索的关键词
  * @param click_category_id  某一个商品品类的ID
  * @param click_product_id   某一个商品的ID
  * @param order_category_ids 一次订单中所有品类的ID集合
  * @param order_product_ids  一次订单中所有商品的ID集合
  * @param pay_category_ids   一次支付中所有品类的ID集合
  * @param pay_product_ids    一次支付中所有商品的ID集合
  * @param city_id            城市ID
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long
                          )

/**
  * 用户信息表
  *
  * @param user_id      用户的ID
  * @param username     用户的名称
  * @param name         用户的名字
  * @param age          用户的年龄
  * @param professional 用户的职业
  * @param city         用户所在的城市
  * @param sex          用户的性别
  */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    city: String,
                    sex: String
                   )

/**
  * 产品表
  *
  * @param product_id   商品的ID
  * @param product_name 商品的名称
  * @param extend_info  商品额外的信息
  */
case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String
                      )

/**
  * 聚合统计表
  *
  * @param taskid                     当前计算批次的ID
  * @param session_count              所有Session的总和
  * @param visit_length_1s_3s_ratio   1-3sSession访问时长占比
  * @param visit_length_4s_6s_ratio   4-6sSession访问时长占比
  * @param visit_length_7s_9s_ratio   7-9sSession访问时长占比
  * @param visit_length_10s_30s_ratio 10-30sSession访问时长占比
  * @param visit_length_30s_60s_ratio 30-60sSession访问时长占比
  * @param visit_length_1m_3m_ratio   1-3mSession访问时长占比
  * @param visit_length_3m_10m_ratio  3-10mSession访问时长占比
  * @param visit_length_10m_30m_ratio 10-30mSession访问时长占比
  * @param visit_length_30m_ratio     30mSession访问时长占比
  * @param step_length_1_3_ratio      1-3步长占比
  * @param step_length_4_6_ratio      4-6步长占比
  * @param step_length_7_9_ratio      7-9步长占比
  * @param step_length_10_30_ratio    10-30步长占比
  * @param step_length_30_60_ratio    30-60步长占比
  * @param step_length_60_ratio       大于60步长占比
  */
case class SessionAggrStat(taskid: String,
                           session_count: Long,
                           visit_length_1s_3s_ratio: Double,
                           visit_length_4s_6s_ratio: Double,
                           visit_length_7s_9s_ratio: Double,
                           visit_length_10s_30s_ratio: Double,
                           visit_length_30s_60s_ratio: Double,
                           visit_length_1m_3m_ratio: Double,
                           visit_length_3m_10m_ratio: Double,
                           visit_length_10m_30m_ratio: Double,
                           visit_length_30m_ratio: Double,
                           step_length_1_3_ratio: Double,
                           step_length_4_6_ratio: Double,
                           step_length_7_9_ratio: Double,
                           step_length_10_30_ratio: Double,
                           step_length_30_60_ratio: Double,
                           step_length_60_ratio: Double
                          )

/**
  * Session随机抽取表
  *
  * @param taskid           当前计算批次的ID
  * @param sessionid        抽取的Session的ID
  * @param startTime        Session的开始时间
  * @param searchKeywords   Session的查询字段
  * @param clickCategoryIds Session点击的类别id集合
  */
case class SessionRandomExtract(taskid: String,
                                sessionid: String,
                                startTime: String,
                                searchKeywords: String,
                                clickCategoryIds: String)

/**
  * top10分类信息
  * @param taskId
  * @param categoryId
  * @param clickCount
  * @param orderCount
  * @param payCount
  */
case class Top10Category(taskId: String,
                               categoryId: Long,
                               clickCount: Long,
                               orderCount: Long,
                               payCount: Long)