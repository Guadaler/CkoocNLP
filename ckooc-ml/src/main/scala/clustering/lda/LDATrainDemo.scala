package clustering.lda
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object LDATrainDemo {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)



    val conf = new SparkConf().setAppName("LDAExample").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val vocabSize = 10000
    val stopwordFile = "ckooc-ml/data/stopword.txt"
    val input = "G:/sample/1999999_split.txt".split(",")

    val ldaUtils = new LDAUtils(sc, sqlContext)

    val preprocessStart = System.nanoTime()
    val tokens = ldaUtils.filter(input, vocabSize, stopwordFile)
    val (documents, vocabArray, actualNumTokens) = ldaUtils.FeatureToVector(tokens, tokens, vocabSize)

    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9
    println(s"预处理时长：$preprocessElapsed sec")
    println()
    ldaUtils.corpusInfo(documents, vocabArray, actualNumTokens)

    val ldaModel = ldaUtils.train(documents, 10, 10)

    val (logLikelihood, logPerplexity) = ldaUtils.evaluation(documents, ldaModel)
    println()
    println(s"\t 似然率：$logLikelihood")
    println(s"\t 混乱率：$logPerplexity")

    val docTopics: RDD[(Long, Vector)] = ldaUtils.docTopics(ldaModel, documents)
    println("文档-主题分布：")
    docTopics.collect().foreach(doc => {
      println(doc._1 + ": " + doc._2)
    })

    val topicWords: Array[Array[(String, Double)]] = ldaUtils.topicWords(ldaModel, vocabArray)
    println("主题-词：")
    topicWords.zipWithIndex.foreach(topic => {
      println("Topic: " + topic._2)
      topic._1.foreach(word => {
        println(word._1 + "\t" + word._2)
      })
      println()
    })

    ldaUtils.saveModel("G:/sample", ldaModel, tokens)

    sc.stop()
  }
}
// scalastyle:on println
