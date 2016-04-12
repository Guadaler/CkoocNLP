package application

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import algorithm.utils.{DistanceUtils, LDAUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 基于LDA的相似文本查找
  * Created by yhao on 2016/3/21.
  */
object LDASimiDocDemo {

  /**
    * 计算文档之间的距离,返回距离
    *
    * @param vector1  向量1
    * @param vector2  向量2
    * @return 距离
    */
  def calcDistance(vector1: Vector[Double], vector2: Vector[Double]): Double = {
    val distanceUtils = new DistanceUtils
    val dist = distanceUtils.cosineDistance(vector1, vector2)

    dist
  }

  /**
    * 保存结果
    * @param dists  结果集
    * @param outFile  要保存的文件路径
    */
  def saveReasult(dists: RDD[(Long, Array[(Long, Double)])], outFile: String) = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile + File.separator + "simiDocs.txt")))
    dists.collect().foreach(doc => {
      val docID = doc._1
      val temp = doc._2.map(pair => pair._1 + ":" + pair._2).mkString(",")
      bw.write(docID + "|" + temp + "\n")
    })

    bw.close()
  }


  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("SimiDocDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ldaUtils = LDAUtils("config/lda.properties")

    val args = Array("../ckooc-nlp/data/sample_split_data1.txt", "../ckooc-nlp/data/sample_split_data2.txt", "G:/test/LDAModel", "G:/test/result")


    val inFile1 = args(0)
    val inFile2 = args(1)
    val modelPath = args(2)
    val outFile = args(3)


    val text1RDD = ldaUtils.getText(sc, inFile1, 36).filter(_.nonEmpty).map(_.split("\\|")).map(line => (line(0).toLong, line(1)))
    val text2RDD = ldaUtils.getText(sc, inFile2, 36).filter(_.nonEmpty).map(_.split("\\|")).map(line => (line(0).toLong, line(1)))

    val (ldaModel, trainTokens) = ldaUtils.loadModel(sc, modelPath)

    val docTopics1 = ldaUtils.predict(text1RDD, ldaModel, trainTokens)._1.map(doc => (doc._1, doc._2.map(_._1)))
    val docTopics2 = ldaUtils.predict(text2RDD, ldaModel, trainTokens)._1.map(doc => (doc._1, doc._2.map(_._1))).collect()

    val dtBc = sc.broadcast(docTopics2)

    val dists = docTopics1.map(doc => {
      val docVector = doc._2.toVector
      val distBuffer = new ArrayBuffer[(Long, Double)]()
      val docArray = dtBc.value

      for (line <- docArray) {
        val lineVector = line._2.toVector
        distBuffer += ((line._1, calcDistance(docVector, lineVector)))
      }
      (doc._1, distBuffer.toArray.sortWith(_._2 > _._2))
    })

    saveReasult(dists, outFile)

    sc.stop()
  }
}
