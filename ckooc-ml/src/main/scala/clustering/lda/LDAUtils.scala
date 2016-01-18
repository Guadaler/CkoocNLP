package clustering.lda

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizerModel, CountVectorizer, StopWordsRemover, RegexTokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by yhao on 2016/1/14.
  */
class LDAUtils {

}

object LDAUtils {
  /**
    * 加载文件, 分词, 创建词汇表, 并将文件准备为term-counts的向量.
    * @return (corpus, vocabulary as array, total token count in corpus)
    *         <p>语料（由文档id以及文档的词汇表大小、词索引、词频组成的稀疏向量组成<p>词汇表（数组）<p>语料中的词数
    */
  def preprocess(
                          sc: SparkContext,
                          paths: Seq[String],
                          vocabSize: Int,
                          stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    // 获取文本文件的数据集
    // 在每个文件中每个文本一行。 如果输入目录包含许多小文件，
    // 将导致非常多的小分区，这些大量的小分区将降低性能。
    // 在此情况下，考虑使用coalesce() 方法创建更少的、更大的分区。
    val df = sc.textFile(paths.mkString(",")).toDF("docs")
    val customizedStopWords: Array[String] = if (stopwordFile.isEmpty) {
      Array.empty[String]
    } else {
      val stopWordText = sc.textFile(stopwordFile).collect()
      stopWordText.flatMap(_.stripMargin.split("\\s+"))
    }
    val tokenizer = new RegexTokenizer()
      .setMinTokenLength(2)
      .setInputCol("docs")
      .setOutputCol("rawTokens")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("rawTokens")
      .setOutputCol("tokens")
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)
    val countVectorizer = new CountVectorizer()
      .setVocabSize(vocabSize)
      .setInputCol("tokens")
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    val model = pipeline.fit(df)
    val documents = model.transform(df)
      .select("features")
      .map { case Row(features: Vector) => features }
      .zipWithIndex()
      .map(_.swap)

    (documents,
      model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary,  // vocabulary
      documents.map(_._2.numActives).sum().toLong) // total token count
  }

  /**
    * 打印 “主题-词” 结果
    * @param topicsDF 包含主题的DataFrame
    * @param vocab  词汇表
    */
  def printTopics(topicsDF: DataFrame, vocab: Array[String]): Unit = {
    val topicsRDD = topicsDF.map( x => (x.getInt(0), x.getSeq[Int](1).toArray, x.getSeq[Double](2).toArray))

    val topics = topicsRDD.map { case (topic, terms, termWeights) =>
      (topic, terms.zip(termWeights).map { case (term, weight) => (vocab(term.toInt), weight) })
    }.sortByKey(ascending = true)

    println("10 topics:")
    topics.foreach { case (topicID, topic) =>
      println(s"TOPIC $topicID")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }
  }
}
