package clustering.lda

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
    val sqlContext = SQLContext.getOrCreate(sc)

    val vocabSize = 10000
    val stopwordFile = "ckooc-ml/data/stopword.txt"
    val input = "G:/sample/1999999_split.txt".split(",")
    val modelPath = "G:/sample"

    val ldaUtils = new LDAUtils(sc, sqlContext)

    val (ldaModel, trainTokens): (LDAModel, DataFrame) = ldaUtils.loadModel(modelPath)
    val tokens = ldaUtils.filter(input, vocabSize, stopwordFile)
    val (documents, vocabArray, actualNumTokens) = ldaUtils.FeatureToVector(tokens, trainTokens, vocabSize)
    val predicted: RDD[(Long, Vector)] = ldaUtils.predict(ldaModel, documents)

    val docTopics: RDD[(Long, Vector)] = ldaUtils.docTopics(ldaModel, predicted)
    println("文档-主题分布：")
    docTopics.collect().foreach(doc => {
      println(doc._1 + ": " + doc._2)
    })

    sc.stop()
  }
}
