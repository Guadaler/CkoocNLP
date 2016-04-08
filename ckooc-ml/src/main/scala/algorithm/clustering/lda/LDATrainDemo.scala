package algorithm.clustering.lda

import algorithm.utils.LDAUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object LDATrainDemo {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("LDATrain").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //加载配置文件
    val ldaUtils = LDAUtils("config/lda.properties")

    val args = Array("../ckooc-nlp/data/preprocess_result.txt", "models/ldaModel")

    val inFile = args(0)
    val outFile = args(1)


    //切分数据
    val textRDD = ldaUtils.getText(sc, inFile, 36).filter(_.nonEmpty).map(_.split("\\|")).map(line => (line(0).toLong, line(1)))

    //训练模型
    val (ldaModel, vocabulary, documents, tokens) = ldaUtils.train(sc, textRDD)

    //计算“文档-主题分布”
    val docTopics: RDD[(Long, Vector)] = ldaUtils.getDocTopics(ldaModel, documents)

    println("文档-主题分布：")
    docTopics.collect().foreach(doc => {
      println(doc._1 + ": " + doc._2)
    })

    //计算“主题-词”
    val topicWords: Array[Array[(String, Double)]] = ldaUtils.getTopicWords(ldaModel, vocabulary.collect())
    println("主题-词：")
    topicWords.zipWithIndex.foreach(topic => {
      println("Topic: " + topic._2)
      topic._1.foreach(word => {
        println(word._1 + "\t" + word._2)
      })
      println()
    })

    //保存模型和训练结果tokens
    ldaUtils.saveModel(sc, outFile, ldaModel, tokens)

    sc.stop()
  }
}
