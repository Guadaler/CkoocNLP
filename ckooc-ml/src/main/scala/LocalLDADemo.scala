import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import lda.LDAUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.MLUtils

/**
 * Created by yhao on 2015/11/24.
 */
object LocalLDADemo {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("LDA").setMaster("local[4]")
      .set("spark.executor.cores", "30")
      .set("spark.executor.memory", "400G")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val args = Array("100", "50", "true")

    val numTopics: Int = args(0).toInt
    val maxIterations: Int = args(1).toInt
    val train = args(2).toBoolean
    val vocabSize: Int = 20000


    val stopwordFile = "data/stopword.txt"
    val ldaUtils = new LDAUtils(conf, sc, sqlContext)
    val mlUtils = new MLUtils

    val input = Seq("G:/out/*")
    val trainOutput = "G:/lda_trained"

    val uuidMap = ldaUtils.generateUUIDMap(input)
    val filteredTokens = ldaUtils.preprocess(input, stopwordFile)

    val (cvModel, countVectors) = mlUtils.FeatureToVector(filteredTokens, vocabSize)

    //训练
    val ldaModel = ldaUtils.train(countVectors, numTopics, maxIterations)
    //打印训练结果
    LDAUtils.printDocIndices(countVectors, ldaModel)
    LDAUtils.printTopicIndices(ldaModel, cvModel)
    //保存结果到文件
    //      LDAUtils.saveResult(trainOutput, countVectors, uuidMap, ldaModel, cvModel)

    //预测
    val predInput = Seq("G:/predict/*")
    val predOutput = "G:/lda_predicted"
    val predUuidMap = ldaUtils.generateUUIDMap(predInput)
    val (loadedModel, loadedFilteredTokens) = (ldaModel, filteredTokens)
    val loadedCvModel = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(loadedFilteredTokens)
    val predFilteredTokens = ldaUtils.preprocess(predInput, stopwordFile)
    val predicted = ldaUtils.predict(predFilteredTokens, loadedModel, loadedCvModel)

    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(predOutput + File.separator + "predicted.txt")))

    for (doc <- predicted.collect()) {
      var str = predUuidMap.get(doc._1).get.split("\\_")(0).split("\\*")(0) + " "
      val topics = doc._2.toArray.zipWithIndex.map(x => (x._2, x._1))
      for (topic <- topics) {
        str += topic._1 + ":" + topic._2 + " "
      }
      bw.write(str.substring(0, str.length - 1) + "\n")
    }

    bw.close()
    sc.stop()
  }
}

