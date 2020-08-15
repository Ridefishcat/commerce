import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, NumberUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object PageStat {



  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID().toString
    val sparkConf = new SparkConf().setAppName("pageStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //获取源数据 【k,v】k:sessionId; v:userVisitAction
    val sessionId2ActionRDD = getActionRDD(sparkSession, taskParam)

    //将1，2，3，4，5，6，7转化成1_2，2_3，3_4，4_5，5_6，6_7 zip函数有些问题
    val pageInfo = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val pageArray = pageInfo.split(",")
    val targetPageFlow = pageArray.slice(0, pageArray.length - 1).zip(pageArray.tail).map{
      case (item1, item2) => item1 + "_" + item2
    }

    //以session为key进行聚合,斧子形数据
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    //将数据转化为格式(page1_page2, 1L)
    val pageId2NumRDD = getPageSplit(targetPageFlow, sessionId2GroupRDD)
    //根据page1_page2计算count数量
    val pageSplitCountMap = pageId2NumRDD.countByKey()

    val starPage = pageArray(0)

    //计算第一个步骤的有多少人
    val startPageCount = sessionId2ActionRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.page_id == starPage.toLong
    }.count()

    //计算单跳转化率，返回一个Map
    val stringToDouble = getPageConvertRate(targetPageFlow, startPageCount, pageSplitCountMap)
    //stringToDouble.foreach(println)
  }

  def getActionRDD(sparkSession: SparkSession,
                   taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam,  Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='"+startDate+"' and date<='" + endDate +"'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item =>(item.session_id,item))

  }

  def getPageSplit(targetPageFlow: Array[String],
                   sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    sessionId2GroupRDD.flatMap{
      case (sessionId,iterableAction) =>
        val sortedAction = iterableAction.toList.sortWith((action01, action02) => {
          DateUtils.parseTime(action01.action_time).getTime < DateUtils.parseTime(action02.action_time).getTime
        })
        val pageInfo = sortedAction.map(item => item.page_id)
        val pageFlow = pageInfo.slice(0,pageInfo.length-1).zip(pageInfo.tail).map{
          case(page1,page2) => page1+"_"+page2
        }
        val pageSplitFiltered = pageFlow.filter(item => targetPageFlow.contains(item)).map(item => (item,1L))
        pageSplitFiltered
    }
  }

  def getPageConvertRate(targetPageFlow: Array[String],
                         startPageCount: Long,
                         pageSplitCountMap:
                         collection.Map[String, Long]) = {
    val pageSplitConvertMap = new mutable.HashMap[String, Double]()
    var lastPageCount = startPageCount.toDouble
    for(page <- targetPageFlow){
      val currentPageCount = pageSplitCountMap.get(page).get.toDouble
      val rate = NumberUtils.formatDouble(currentPageCount / lastPageCount, 2)
      pageSplitConvertMap.put(page,rate)
      lastPageCount = currentPageCount
    }
    pageSplitConvertMap
  }



}
