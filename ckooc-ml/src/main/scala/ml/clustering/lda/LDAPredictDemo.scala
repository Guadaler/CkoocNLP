package ml.clustering.lda

import _root_.utils.LDAUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yhao on 2016/1/21.
  */
object LDAPredictDemo {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("LDA").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ldaUtils = LDAUtils("config/lda.properties")

    val vocabSize = 10000
    val inFile = "data/preprocess_result.txt"
    val modelPath = "G:/test/LDAModel"

    val textRDD = sc.textFile(inFile).filter(_.nonEmpty)

    val (ldaModel, trainTokens, vocabRDD): (LDAModel, DataFrame, RDD[String]) = ldaUtils.loadModel(sc, modelPath)
    val tokens = ldaUtils.splitLine(sc, textRDD, vocabSize)

    val documents = ldaUtils.featureToVector(tokens, trainTokens, vocabSize)._1

    val docTopics: RDD[(Long, Vector)] = ldaUtils.docTopics(ldaModel, documents)
    println("文档-主题分布：")
    docTopics.collect().foreach(doc => {
      println(doc._1 + ": " + doc._2)
    })

    val topicWords: Array[Array[(String, Double)]] = ldaUtils.topicWords(ldaModel, vocabRDD.collect())
    println("主题-词：")
    topicWords.zipWithIndex.foreach(topic => {
      println("Topic: " + topic._2)
      topic._1.foreach(word => {
        println(word._1 + "\t" + word._2)
      })
      println()
    })

    sc.stop()
  }
}
