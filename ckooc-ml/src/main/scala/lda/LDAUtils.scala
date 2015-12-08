package lda

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class LDAUtils(conf: SparkConf, sc: SparkContext, sqlContext: SQLContext) {

  /**
   * 生成UUID和ID的map
   * @param path  输入数据
   * @return  包含uuid的hashmap
   */
  def generateUUIDMap(path: Seq[String]): mutable.HashMap[Long, String] = {
    val files = sc.textFile(path.mkString(",")).collect()
    val uuidMap = new mutable.HashMap[Long, String]
    var id = 0L
    for (i <- 0 until files.length) {
      if (files(i).split("\\|")(0).split("\\_").length == 2) {
        id = files(i).split("\\|")(0).split("\\_")(1).toLong
        uuidMap.put(id, files(i).split("\\|")(0))
      } else {
        println("uuid格式错误！")
      }
    }
    uuidMap
  }

  /**
   * 预处理操作
   * @param input 输入数据
   * @param stopwordFile  停用词文件
   * @return  向量模型和词组成的向量
   */
  def preprocess(input: Seq[String], stopwordFile: String): DataFrame = {
    val mlUtils = new MLUtils
    val rawTextRDD = sc.textFile(input.mkString(","))

    import sqlContext.implicits._

    val docDF = rawTextRDD.filter(doc => doc.length > 38).map(doc => {
      val docId = doc.split("\\|")(0).split("\\_")(1).toLong
      val text = doc.split("\\|")(1)
      (text, docId)
    }).toDF("text", "docId")

    val tokens = mlUtils.tokenlizeLines(docDF)

    val stopwords: Array[String] = sc.textFile(stopwordFile).collect()
    mlUtils.removeStopwords(tokens, stopwords)
  }


  /**
   * 训练LDA
   * @param countVectors  词组成的向量
   * @param numTopics 主题数
   * @param maxIterations 迭代次数
   * @return  LDA模型
   */
  def train(countVectors: RDD[(Long, Vector)], numTopics: Int, maxIterations: Int): LDAModel = {

    //根据语料库大小划分LDA处理的Batch大小
    val mbf = {
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      val corpusSize = countVectors.count()
      2.0 / maxIterations + 1.0 / corpusSize
    }
    val lda = new LDA()
      .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(math.min(1.0, mbf)))
      .setK(numTopics)
      .setMaxIterations(maxIterations)
      .setDocConcentration(-1) // use default symmetric document-topic prior
      .setTopicConcentration(-1) // use default symmetric topic-word prior

    val startTime = System.nanoTime()
    val ldaModel = lda.run(countVectors)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.  Summary:")
    println(s"Training time (sec)\t$elapsed")
    println(s"==========")

    ldaModel
  }

  /**
   * 预测新文档
   * @param predFilteredTokens  词组成的向量
   * @param ldaModel  LDA模型
   * @param cvModel 向量模型
   * @return  “文档-主题分布”结果
   */
  def predict(predFilteredTokens: DataFrame, ldaModel: LDAModel, cvModel: CountVectorizerModel): RDD[(Long, Vector)] = {
    val countVectors = cvModel.transform(predFilteredTokens)
      .select("docId", "features")
      .map { case Row(docId: Long, countVector: Vector) => (docId, countVector) }
    var predicted: RDD[(Long, Vector)] = null
    ldaModel match {
      case localModel: LocalLDAModel =>
        predicted = localModel.topicDistributions(countVectors)

      case _ =>
    }
    predicted
  }

  //保存LDA模型和词典数据
  def save(modelPath: String, ldaModel: LDAModel, filteredTokens: DataFrame): Unit ={
    val startTime = System.currentTimeMillis()
    ldaModel.save(sc, modelPath + File.separator + "LDAModel")
    filteredTokens.write.parquet(modelPath + File.separator + "tokens")
    val totalTime = System.currentTimeMillis() - startTime
    println(s"====== 保存模型成功！\n\t耗时：$totalTime")
  }

  //加载LDA模型和词典数据
  def load(modelPath: String): (LDAModel, DataFrame) = {
    val ldaModel = LocalLDAModel.load(sc, modelPath + File.separator + "LDAModel")
    val filteredTokens = sqlContext.read.parquet(modelPath + File.separator + "tokens")
    (ldaModel, filteredTokens)
  }
}


object LDAUtils {

  def saveResult(output: String, countVectors: RDD[(Long, Vector)], uuidMap: mutable.HashMap[Long, String], ldaModel: LDAModel, cvModel: CountVectorizerModel): Unit ={
    val bwDoc = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output + File.separator + "DocIndices.txt")))
    val bwTopic = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output + File.separator + "TopicIndices.txt")))

    ldaModel match {
      case localModel: LocalLDAModel =>
        val docIndices = localModel.topicDistributions(countVectors).collect()
        for (doc <- docIndices) {
          val topics = doc._2.toArray.zipWithIndex.map(x => (x._2, x._1))
          var str = uuidMap.get(doc._1).get.split("\\_")(0).split("\\*")(1) + " "
          for (topic <- topics) {
            str += topic._1 + ":" + topic._2 + " "
          }
          bwDoc.write(str.substring(0, str.length - 1) + "\n")
        }
      case _ =>
    }

    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val vocabArray = cvModel.vocabulary
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabArray(_)).zip(termWeights)
    }
    topics.zipWithIndex.foreach { case (topic, i) =>
      var str = s"Topic$i:["
      topic.foreach { case (term, weight) => str +=term + ":" + weight + "," }
      bwTopic.write(str.substring(0, str.length - 1) + "]\n")
    }

    bwDoc.close()
    bwTopic.close()
  }

  /**
   * 打印“文档-主题分布”
   * @param countVectors  词组成的向量
   * @param ldaModel  LDA模型
   */
  def printDocIndices(countVectors: RDD[(Long, Vector)], ldaModel: LDAModel): Unit = {
    //输出文档-主题分布
    ldaModel match {
      case localModel: LocalLDAModel =>
        val docIndices = localModel.topicDistributions(countVectors).collect()
        println(docIndices.length + " documents:")
        for (doc <- docIndices) {
          println(doc._1 + ":" + doc._2)
        }
      case _ =>
    }
  }

  /**
   * 打印“主题-词分布”
   * @param ldaModel  LDA模型
   * @param cvModel 向量模型
   */
  def printTopicIndices(ldaModel: LDAModel, cvModel: CountVectorizerModel): Unit = {

    // 输出主题-词分布，显示每个主题下权重靠前的词
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val vocabArray = cvModel.vocabulary
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabArray(_)).zip(termWeights)
    }
    println("\n" + topics.length + " topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) => println(s"$term\t$weight") }
      println(s"==========")
    }
  }
}