import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SessionStat {

  def main(args: Array[String]): Unit = {
    //读取配置文件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //把json字符串转化成json对象
    val taskParam = JSONObject.fromObject(jsonStr)
    //创建全局唯一的ID
    val taskUUID = UUID.randomUUID().toString
    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")
    //创建sparkSession 包含sparkContext
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //获取原始数据 返回的是RDD[UserVisitAction]
    val actionRDD = getOriActionRDD(sparkSession, taskParam)
    // 做一个映射 把RDD[UserVisitAction]转化为kv的格式RDD[(sessionId, UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id,item))
    //按照key进行聚合 key:session_id value:iterate[UserVisitAction]
    val session2GroupActionRDD = sessionId2ActionRDD.groupByKey()
    //应该是加到缓存
    session2GroupActionRDD.cache()
    //聚合成一条数据 算出用户访问开始结束时间 搜索关键词 点击品类等
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, session2GroupActionRDD)
    //创建一个累加器对象,注册到spark中
    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)
    //按json条件过滤
    val sessionId2FilterRDD = getSessionFilteredRDD(taskParam,sessionId2FullInfoRDD,sessionAccumulator)
    //sessionId2FilterRDD.foreach(println(_))

    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)

    //获取所有符合过滤条件的action数据
    val sessionId2FilterActionRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map{
      case (sessionId,(action,fullInfo)) =>
        (sessionId,action)
    }

    //需求三：计算top10品类数据
    val top10CategoryArray = top10PopularCategories(sparkSession,taskUUID,sessionId2FilterActionRDD)
    //top10CategoryArray.foreach(println(_))

    //需求四：Top10热门品类的Top10活跃session统计
    top10ActiveSession(sparkSession,taskUUID,sessionId2FilterActionRDD,top10CategoryArray)

  }

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

    val stat = SessionAggrStat(
          taskUUID, session_count.toInt,
          visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
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
          .option("dbtable", "session_aggr_stat")
          .mode(SaveMode.Append)
          .save()
  }

  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: SessionAccumulator): Unit = {
    if(visitLength>=1 && visitLength<=3){
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    }else if(visitLength >=4 && visitLength  <= 6){
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionAccumulator): Unit = {
    if(stepLength >=1 && stepLength <=3){
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilteredRDD(taskParam: JSONObject,
                            sessionId2FullInfoRDD: RDD[(String, String)],
                            sessionAccumulator: SessionAccumulator): RDD[(String, String)] = {
    //获取过滤条件
    val startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)
    //进行拼接
    var filterInfo = (if(startAge != null) Constants.PARAM_START_AGE + "=" +startAge + "|"  else "")+
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")
    //处理结尾字符串
    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0,filterInfo.length-1)
    //进行过滤
    val sessionId2FilterRDD = sessionId2FullInfoRDD.filter{
      case (sessionId,fullInfo) =>
        var success = true;
        if(!ValidUtils.between(fullInfo,Constants.FIELD_AGE,filterInfo,Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
          success = false
        }else if(!ValidUtils.in(fullInfo,Constants.FIELD_PROFESSIONAL,filterInfo,Constants.FIELD_PROFESSIONAL)){
          success = false;
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)){
          success = false
        }else if(!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)){
          success = false
        }

        if(success){
          sessionAccumulator.add(Constants.SESSION_COUNT)

          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)
        }
        success
    }
    sessionId2FilterRDD
  }

  def getSessionFullInfo(sparkSession: SparkSession,
                         session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]): RDD[(String, String)] = {
    val userId2AggrInfoRDD =session2GroupActionRDD.map{
      case (sessionId,iterableAction) =>
        var userId = -1L
        var startTime:Date = null
        var endTime:Date = null
        var stepLength = 0
        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")
        for(action <- iterableAction){
          if(userId == -1L){
            userId = action.user_id
          }
          val actionTime = DateUtils.parseTime(action.action_time)
          //找到startTime
          if(startTime==null || startTime.after(actionTime)){
            startTime = actionTime
          }
          //找到endTime
          if(endTime==null || endTime.before(actionTime)){
            endTime = actionTime
          }
          //搜索关键词
          val searchKeyword = action.search_keyword
          if(StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)){
            searchKeywords.append(searchKeyword+",")
          }
          //点击品类ID
          val clickCategoryId = action.click_category_id
          if(clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)){
            clickCategories.append(clickCategoryId + ",")
          }
          //有多少条记录
          stepLength += 1
        }
        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)
        //用户访问时间
        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        //输出形式
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, aggrInfo)
    }
    val sql = "select * from user_info"
    //隐式转换
    import sparkSession.implicits._

    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    //两张表进行连接，拼接成我们想要的格式
    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2InfoRDD).map{
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

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
    sessionId2FullInfoRDD
  }

  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    //拼接sql语句
    val sql = "select * from user_visit_action where date >='"+startDate +"' and date <='" + endDate + "'"
    //隐式转换
    import  sparkSession.implicits._
    //执行sql转化成UserVisitAction格式的RDD
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }


  def top10PopularCategories(sparkSession: SparkSession,
                             taskUUID: String,
                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    //第一步：获取所有发生过点击，下单，付款品类
    var cid2CidRDD = sessionId2FilterActionRDD.flatMap{
      case (sid,action) =>
        val categoryBuffer = new ArrayBuffer[(Long,Long)]()
        if(action.click_category_id != -1){
          categoryBuffer += ((action.click_category_id,action.click_category_id))
        }else if(action.order_category_ids != null){
          for(orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong,orderCid.toLong))
        }else if(action.pay_category_ids != null){
          for(payCid <- action.pay_category_ids.split(","))
            categoryBuffer+= ((payCid.toLong,payCid.toLong))
        }
        categoryBuffer
    }

    //去重
    cid2CidRDD = cid2CidRDD.distinct()

    // 第二步：统计品类的点击次数、下单次数、付款次数
    val cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)
    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)
    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)

    //打印输出格式为：(59,categoryid=59|clickCount=58|orderCount=63|payCount=67)
    val cid2FullCountRDD = getFullCount(cid2CidRDD,cid2ClickCountRDD,cid2OrderCountRDD,cid2PayCountRDD)

    val sortKey2FullCountRDD = cid2FullCountRDD.map{
      case (cid,countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey,countInfo)
    }

    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)

    top10CategoryArray
  }

  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val clickFilterRDD = sessionId2FilterActionRDD.filter{
      case (sessionId,action) => action.click_category_id != -1L
    }
    val clickNumRDD = clickFilterRDD.map{
      case (sessionId,action) => (action.click_category_id,1L)
    }
    clickNumRDD.reduceByKey(_+_)
  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)
    val orderNumRDD = orderFilterRDD.flatMap{
      case (sessionId,action) => action.order_category_ids.split(",")
        .map(item => (item.toLong,1L))
    }
    orderNumRDD.reduceByKey(_+_)
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)

    val payNumRDD = payFilterRDD.flatMap{
      case (sid, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    payNumRDD.reduceByKey(_+_)
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map{
      case (cid,(categoryId,option)) =>
        val clickCount = if(option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid,aggrCount)
    }
    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map{
      case (cid, (clickInfo, option)) =>
        val orderCount = if(option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
    }
    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map{
      case (cid, (orderInfo, option)) =>
        val payCount = if(option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
    }
    cid2PayInfoRDD
  }

  def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]) = {
    //第一步：过滤出所有点击过top10品类的action
    //cidArray:Array[Long]
    val cidArray = top10CategoryArray.map{
      case (sortKey,countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
        cid
    }
    //包含top10品类的所有action
    val sessionId2ActionRDD = sessionId2FilterActionRDD.filter{
      case (sessionId,action) =>
        cidArray.contains(action.click_category_id)
    }
    //按照sessionId进行聚合
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

  }


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

}
