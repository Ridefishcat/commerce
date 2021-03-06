import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("areaStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //创建sparkStreaming相关的
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    //从配置文件导入kafka的brokers和topics
    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKER_LIST)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
    //kafka的一些参数
    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      //均匀的分发给消费者，也就是Excutor
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )
    //kafka的数据发过来是key value 我们key存的是空，所以我们只要value
    val adReadTimeValueDStream = adRealTimeDStream.map(item => item.value())

    //从mysql中读数据，进行了黑名单过滤，根据userId
    val adRealTimeFilterDStream = adReadTimeValueDStream.transform{
      logRDD =>
        val blackListArray = AdBlacklistDAO.findAll()
        val userIdArray = blackListArray.map(item => item.userid)
        logRDD.filter{
          case log =>
            val logSplit = log.split("")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }


    //adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println(_)))
    //需求一：实时维护黑名单
    generateBlackList(adRealTimeFilterDStream)

    // 需求二：各省各城市一天中的广告点击量（累积统计）
    val key2ProvinceCityCountDStream = provinceCityClickStat(adRealTimeFilterDStream)

    // 需求三：统计各省Top3热门广告
    proveinceTope3Adver(sparkSession, key2ProvinceCityCountDStream)

    // 需求四：最近一个小时广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)



    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val key2TimeMinuteDStream = adRealTimeFilterDStream.map{
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // yyyyMMddHHmm
        val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adid = logSplit(4).toLong

        val key = timeMinute + "_" + adid
        (key,1L)
    }
    val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a:Long, b:Long)=>(a+b), Minutes(60), Minutes(1))

    key2WindowDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        // (key, count)
        items=>
          val trendArray = new ArrayBuffer[AdClickTrend]()
          for((key, count) <- items){
            val keySplit = key.split("_")
            // yyyyMMddHHmm
            val timeMinute = keySplit(0)
            val date = timeMinute.substring(0, 8)
            val hour = timeMinute.substring(8,10)
            val minute = timeMinute.substring(10)
            val adid  = keySplit(1).toLong

            trendArray += AdClickTrend(date, hour, minute, adid, count)
          }
          AdClickTrendDAO.updateBatch(trendArray.toArray)
      }
    }

  }


  def proveinceTope3Adver(sparkSession: SparkSession,
                          key2ProvinceCityCountDStream: DStream[(String, Long)]) = {
    // key2ProvinceCityCountDStream: [RDD[(key, count)]]
    // key: date_province_city_adid
    // key2ProvinceCountDStream: [RDD[(newKey, count)]]
    // newKey: date_province_adid
    val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map{
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)

        val newKey = date + "_" + province + "_" + adid
        (newKey, count)
    }

    val key2ProvinceAggrCountDStream = key2ProvinceCountDStream.reduceByKey(_+_)

    val top3DStream = key2ProvinceAggrCountDStream.transform{
      rdd =>
        // rdd:RDD[(key, count)]
        // key: date_province_adid
        val basicDateRDD = rdd.map{
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong

            (date, province, adid, count)
        }

        import sparkSession.implicits._
        basicDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date, province, adid, count from(" +
          "select date, province, adid, count, " +
          "row_number() over(partition by date,province order by count desc) rank from tmp_basic_info) t " +
          "where rank <= 3"

        sparkSession.sql(sql).rdd
    }

    top3DStream.foreachRDD{
      // rdd : RDD[row]
      rdd =>
        rdd.foreachPartition{
          // items : row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for(item <- items){
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }
  }



  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    // adRealTimeFilterDStream: DStream[RDD[String]]    String -> log : timestamp province city userid adid
    // key2ProvinceCityDStream: DStream[RDD[key, 1L]]
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map{
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // dateKey : yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val adid = logSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }

    // key2StateDStream： 某一天一个省的一个城市中某一个广告的点击次数（累积）
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long]{
      (values:Seq[Long], state:Option[Long]) =>
        var newValue = 0L
        if(state.isDefined)
          newValue = state.get
        for(value <- values){
          newValue += value
        }
        Some(newValue)
    }

    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val adStatArray = new ArrayBuffer[AdStat]()
          // key: date province city adid
          for((key, count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adid = keySplit(3).toLong

            adStatArray += AdStat(date, province, city, adid, count)
          }
          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    }

    key2StateDStream
  }


  //构建（key，1L）
  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    val key2NumDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adId = logSplit(4).toLong
        val key = dateKey + "_" + userId + "_" + adId
        (key, 1L)
    }
    //进行聚合
    val key2CountDStream = key2NumDStream.reduceByKey(_ + _)
    //根据每一个RDD数据，更新用户点击次数表
    key2CountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()
          for((key,count)<- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val userId = keySplit(1).toLong
            val adId = keySplit(2).toLong
            clickCountArray += AdUserClickCount(date,userId,adId,count)
          }
          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }

    //走黑名单过滤一下
    val key2BlackListDStream = key2CountDStream.filter{
      case(key,count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adId = keySplit(2).toLong
        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date,userId,adId)
        if(clickCount>10){
          true
        }else{
          false
        }
    }

    val userIdDStream = key2BlackListDStream.map{
      case (key,count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()
          for(userId <- items){
            userIdArray += AdBlacklist(userId)
          }
          AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }
    }

  }
}
