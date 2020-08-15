import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AreaTop3Stat {

  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    val sparkConf = new SparkConf().setAppName("areaStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //RDD[cityId,clickProductId]
    val cityId2PidRDD = getCityAndProductInfo(sparkSession, taskParam)

    //RDD[cityId,cityInfo(cityId,cityName,area)]
    val cityId2AreaInfoRDD = getCityAreaInfo(sparkSession)
    //生成一张tmp_area_basic_info表 字段包含："city_id","city_name","area","pid"
    getAreaPidBasicInfoTable(sparkSession, cityId2PidRDD, cityId2AreaInfoRDD)
    //sparkSession.sql("select * from tmp_area_basic_info").show()

    sparkSession.udf.register("concat_long_String",(v1:Long,v2:String,split:String)=>{
      v1 + split + v2
    })
    //注册自定的UDAF函数
    sparkSession.udf.register("group_concat_distinct",new GroupConcatDistinct)

    //创建tmp_area_click_count表 |area|pid|count(1)|city_infos| |  华中| 40| 24| 5:武汉,6:长沙|
    getAreaProductClickCountTable(sparkSession)

    sparkSession.udf.register("get_json_field",(json:String,field:String)=>{
      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(field)
    })
    getAreaProductClickCountInfo(sparkSession: SparkSession)
    getTop3Product(sparkSession)
    sparkSession.sql("select * from tmp_top3").show()

  }

  def getCityAndProductInfo(sparkSession: SparkSession,
                            taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam,  Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select city_id,click_product_id from user_visit_action where date>='"+startDate+"' " +
      "and date<='" + endDate +"' and click_product_id != -1"
    println(sql)
    import sparkSession.implicits._
    sparkSession.sql(sql).as[CityClickProduct].rdd.map{
      case cityPid =>(cityPid.city_id,cityPid.click_product_id)
    }
  }

  def getCityAreaInfo(sparkSession: SparkSession) = {
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))
    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map{
      case(cityId,cityName,area) => {
        (cityId,CityAreaInfo(cityId,cityName,area))
      }
    }
  }

  def getAreaPidBasicInfoTable(sparkSession: SparkSession,
                               cityId2PidRDD: RDD[(Long, Long)],
                               cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]) = {
    val areaPidInfoRDD = cityId2PidRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, areaInfo)) => {
        (cityId, areaInfo.city_name, areaInfo.area, pid)
      }
    }
    import sparkSession.implicits._
    areaPidInfoRDD.toDF("city_id","city_name","area","pid").createOrReplaceTempView("tmp_area_basic_info")
  }

  def getAreaProductClickCountTable(sparkSession: SparkSession) = {
    val sql = "select area,pid,count(*) click_count,"+
    " group_concat_distinct(concat_long_string(city_id, city_name,':')) city_infos"+
    " from tmp_area_basic_info group by area,pid"
    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count")
  }

  def getAreaProductClickCountInfo(sparkSession: SparkSession) = {
    val sql = "select tacc.area, tacc.city_infos, tacc.pid, pi.product_name, " +
      "if(get_json_field(pi.extend_info,'product_status')=='0','Self','Third Party') product_status,"+
      "click_count "+
      " from tmp_area_click_count tacc join product_info pi on tacc.pid = pi.product_id"
    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")
  }

  def getTop3Product(sparkSession: SparkSession) = {
    //group by area 报错
    val sql = "select * from tmp_area_count_product_info order by click_count desc"
    //用了开窗函数，加了个序号，然后后面根据这个序号，算出top3
    val sql01 = "select area, city_infos, pid, product_name, product_status, click_count, " +
      "row_number() over(PARTITION BY area ORDER BY click_count desc) rank from tmp_area_count_product_info"
    val sql02 = "select area, " +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A_Level' " +
      "WHEN area='华中' OR area='华南' THEN 'B_Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C_Level' " +
      "ELSE 'D_Level' " +
      "END area_level, " +
      "city_infos, pid, product_name, product_status, click_count from (" +
      "select area, city_infos, pid, product_name, product_status, click_count, " +
      "row_number() over(PARTITION BY area ORDER BY click_count DESC) rank from " +
      "tmp_area_count_product_info) t where rank<=3"
    sparkSession.sql(sql02).createOrReplaceTempView("tmp_top3")
  }
}
