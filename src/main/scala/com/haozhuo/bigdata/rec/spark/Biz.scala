package com.haozhuo.bigdata.rec.spark

import com.haozhuo.bigdata.rec.Props
import com.haozhuo.bigdata.rec.bean.Users
import com.haozhuo.bigdata.rec.hive.hcatalog.HiveClient
import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by hadoop on 5/24/17.
 */
object Biz {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Props.get("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test_consumer_group_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> true
    )
    val topics = Array(Props.get("kafka.topic.name"))
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    stream.map(record => record.value.split(","))
      .filter(x => x.length == 2 && StringUtils.isNumeric(x(0)))
      .map(x => new Users(x(0).toInt, x(1)))
      .repartition(1)
      .foreachRDD {
        rdd =>
          val result = rdd.collect()
          if (result.length > 0) {
            val hiveClient = new HiveClient()
            hiveClient.insertData(result)
            hiveClient.close()
          }

      }
    /*      .foreachRDD{
          rdd =>
          // Get the singleton instance of SparkSession
          val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
          import spark.implicits._

          // Convert RDD[String] to DataFrame
          val wordsDataFrame = rdd.toDF("value")

          // Create a temporary view
          wordsDataFrame.createOrReplaceTempView("words")

          // Do word count on DataFrame using SQL and print it
          val wordCountsDataFrame =
            spark.sql("select * from words")
          wordCountsDataFrame.show()
        }*/
    ssc.start()
    ssc.awaitTermination()

  }
}
