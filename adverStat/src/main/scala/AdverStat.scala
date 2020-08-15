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

    streamingContext.start()
    streamingContext.awaitTermination()
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
