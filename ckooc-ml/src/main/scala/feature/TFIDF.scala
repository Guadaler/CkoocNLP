package feature

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector


/**
 * Created by yhao on 2015/12/25.
 */

object TFIDF {
  def train(sc: SparkContext, input: RDD[String]): RDD[Vector] = {
    val documents = input.map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(documents)

    tf.cache()

    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    tfidf
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TF-IDF").setMaster("local")
    val sc = new SparkContext(conf)

    val inputPath = "G:\\data\\新闻分类数据.dat"
    val input = sc.textFile(inputPath)
    val tfidf = this.train(sc, input)
    println(tfidf.first())
  }
}
