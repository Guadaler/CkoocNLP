package scala.spark.mllib.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yhao on 2016/4/8.
  */
object LDADemo {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("LDA-Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 加载并解析数据
    val data = sc.textFile("data/clustering/sample_lda_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))

    // 对文档建立唯一索引
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // 使用LDA训练使文档按主题聚为3类
    val ldaModel = new LDA().setK(3).run(corpus)

    // 打印主题。每一个主题都是词的分布
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }

    // 保存、加载模型
    ldaModel.save(sc, "myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "myLDAModel")
  }
}
