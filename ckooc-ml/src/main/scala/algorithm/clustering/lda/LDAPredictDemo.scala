package algorithm.clustering.lda

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import algorithm.utils.LDAUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * LDA新文档预测Demo
  * Created by yhao on 2016/1/21.
  */
object LDAPredictDemo {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("LDA-Predict").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ldaUtils = LDAUtils("ckooc-ml/config/lda.properties")

    val args = Array("ckooc-ml/data/preprocess_result.txt", "G:/test/LDAModel", "G:/test/result")

    val inFile = args(0)
    val modelPath = args(1)
    val outFile = args(2)

    //切分数据
    val textRDD = sc.textFile(inFile).filter(_.nonEmpty).map(_.split("\\|")).map(line => (line(0).toLong, line(1)))

    //加载LDAModel
    val (ldaModel, trainTokens) = ldaUtils.loadModel(sc, modelPath)

    //预测文档，得到“文档-主题分布”和“主题-词”结果
    val (docTopics, topicWords) = ldaUtils.predict(sc, textRDD, ldaModel, trainTokens)

    println("文档-主题分布：")
    docTopics.collect().foreach(doc => {
      println(doc._1 + ": " + doc._2)
    })

    println("主题-词：")
    topicWords.zipWithIndex.foreach(topic => {
      println("Topic: " + topic._2)
      topic._1.foreach(word => {
        println(word._1 + "\t" + word._2)
      })
      println()
    })

    //保存结果
    saveReasult(docTopics, topicWords, outFile)

    sc.stop()
  }


  /**
    * 保存预测结果
    * @param docTopics  文档-主题分布
    * @param topicWords 主题-词
    * @param outFile  输出路径
    */
  def saveReasult(docTopics: RDD[(Long, Vector)], topicWords: Array[Array[(String, Double)]], outFile: String): Unit = {
    val bw1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile + File.separator + "docTopics.txt")))
    val bw2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile + File.separator + "topicWords.txt")))

    docTopics.collect().foreach(doc => {
      bw1.write(doc._1 + ": " + doc._2 + "\n")
    })

    topicWords.zipWithIndex.foreach(topic => {
      bw2.write("\n\nTopic: " + topic._2 + "\n")
      topic._1.foreach(word => {
        bw2.write(word._1 + "\t" + word._2 + "\n")
      })
      println()
    })

    bw1.close()
    bw2.close()
  }
}
