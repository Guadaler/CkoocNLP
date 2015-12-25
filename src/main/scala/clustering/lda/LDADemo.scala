package clustering.lda

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.MLUtils

/**
 * Created by yhao on 2015/11/24.
 */
object LDADemo {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("LDA")//.setMaster("local")
      .set("spark.executor.cores", "40")
      .set("spark.executor.memory", "400G")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val numTopics: Int = args(0).toInt
    val maxIterations: Int = args(1).toInt
    val train = args(2).toBoolean
    val vocabSize: Int = 20000


    val stopwordFile = "hdfs://systex:/data/corpus/yhao/stopword.txt"
    val modelPath = "hdfs://systex:/data/corpus/yhao/LDAModel"
    val ldaUtils = new LDAUtils(conf, sc, sqlContext)
    val mlUtils = new MLUtils

    if (train) {
      val input = Seq(args(3))
      val output = "/home/spark/yhao/SparkLDA/output/train"

      val uuidMap = ldaUtils.generateUUIDMap(input)
      val filteredTokens = ldaUtils.preprocess(input, stopwordFile)

      val (cvModel, countVectors) = mlUtils.FeatureToVector(filteredTokens, vocabSize)

      //训练
      val ldaModel = ldaUtils.train(countVectors, numTopics, maxIterations)
      //打印训练结果
//          LDAUtils.printDocIndices(countVectors, ldaModel)
//          LDAUtils.printTopicIndices(ldaModel, cvModel)
      //保存结果到文件
      LDAUtils.saveResult(output, countVectors, uuidMap, ldaModel, cvModel)
      //保存模型
      ldaUtils.save(modelPath, ldaModel, filteredTokens)
    } else {

      //预测
      val predInput = Seq(args(3))
      val output = "/home/spark/yhao/SparkLDA/output/predict"
      val uuidMap = ldaUtils.generateUUIDMap(predInput)
      //加载模型
      val (loadedModel, loadedFilteredTokens) = ldaUtils.load(modelPath)
      val loadedCvModel = new CountVectorizer()
        .setInputCol("filtered")
        .setOutputCol("features")
        .setVocabSize(vocabSize)
        .fit(loadedFilteredTokens)
      val predFilteredTokens = ldaUtils.preprocess(predInput, stopwordFile)
      val predicted = ldaUtils.predict(predFilteredTokens, loadedModel, loadedCvModel)

      val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output + File.separator + "predicted.txt")))

      for (doc <- predicted.collect()) {
        var str = uuidMap.get(doc._1).get.split("\\_")(0).split("\\*")(1) + " "
        val topics = doc._2.toArray.zipWithIndex.map(x => (x._2, x._1))
        for (topic <- topics) {
          str += topic._1 + ":" + topic._2 + " "
        }
        bw.write(str.substring(0, str.length - 1) + "\n")
      }

      bw.close()
    }

    sc.stop()
  }
}
