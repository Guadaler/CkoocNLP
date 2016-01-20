package clustering.lda
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object LDAExample {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("LDAExample").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val vocabSize = 10000
    val stopwordFile = "ckooc-ml/data/stopword.txt"
    val input = "G:/sample/1999999_split.txt".split(",")

    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens) = LDAUtils.preprocess(sc, input, vocabSize, stopwordFile)
    corpus.cache()
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9
    println(s"\t 预处理时长：$preprocessElapsed sec")
    println()
    LDAUtils.corpusInfo(corpus, vocabArray, actualNumTokens)

    val ldaModel = LDAUtils.train(corpus, 10, 10)

    val (logLikelihood, logPerplexity) = LDAUtils.evaluation(corpus, ldaModel)
    println()
    println(s"\t 似然率：$logLikelihood")
    println(s"\t 混乱率：$logPerplexity")

    val docTopics: RDD[(Long, Vector)] = LDAUtils.docTopics(ldaModel, corpus)
    println("文档-主题分布：")
    docTopics.collect().foreach(doc => {
      println(doc._1 + ": " + doc._2)
    })

    val topicWords: Array[Array[(String, Double)]] = LDAUtils.topicWords(ldaModel, vocabArray)
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
// scalastyle:on println
