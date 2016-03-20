package utils

import java.io.File
import java.util.Properties

import conf.LDAConfig
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Logging, SparkContext}

/**
  * Created by yhao on 2016/1/20.
  */
class LDAUtils(config: LDAConfig) extends Logging with Serializable {

  /**
    * 选择算法
    *
    * @param algorithm  算法名（EM或者ONLINE）
    * @param corpusSize 语料库大小
    * @return LDA优化器
    */
  private def selectOptimizer(algorithm: String, corpusSize: Long): LDAOptimizer = {
    val optimizer = algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / corpusSize)
      case _ => throw new IllegalArgumentException(
        s"只支持：em 和 online算法，输入的是： $algorithm.")
    }

    optimizer
  }


  /**
    * 读取数据文件(已分词)，切分
    *
    * @param sc SparkContext
    * @param textRDD  数据(已分词)
    * @param vocabSize  词汇表大小
    * @return (corpus, vocabArray, actualNumTokens)
    */
  def splitLine(sc: SparkContext, textRDD: RDD[String], vocabSize: Int): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(sc)

    import sqlContext.implicits._

    val df = textRDD.toDF("docs")

    val tokenizer = new RegexTokenizer()
      .setMinTokenLength(2)
      .setInputCol("docs")
      .setOutputCol("tokens")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer))

    val model = pipeline.fit(df)
    val tokens = model.transform(df)
    tokens
  }


  /**
    * 限制vocabSize个词频最高的词形成词汇表，将词频转化为特征向量
    *
    * @param filteredTokens 过滤后的tokens,作为输入数据
    * @param trainTokens 过滤后的tokens，用于cvModel模型训练
    * @param vocabSize  词汇表大小
    * @return
    */
  def featureToVector(filteredTokens: DataFrame, trainTokens: DataFrame, vocabSize: Int): (RDD[(Long, Vector)], Array[String], Long) = {
    val cvModel = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(trainTokens)
    val documents = cvModel.transform(filteredTokens)
      .select("features")
      .map { case Row(features: Vector) => features }
      .zipWithIndex()
      .map(_.swap)
    (documents, cvModel.vocabulary, documents.map(_._2.numActives).sum().toLong)
  }

  /**
    * 打印特征转换相关信息
    *
    * @param actualCorpusSize  实际语料大小
    * @param actualVocabSize   实际词汇表大小
    * @param actualNumTokens   实际token数量
    * @param preprocessElapsed 转换特征耗时
    */
  def featureInfo(actualCorpusSize: Long, actualVocabSize: Int, actualNumTokens: Long, preprocessElapsed: Double) = {
    println()
    println(s"语料信息：")
    println(s"\t 训练集大小：$actualCorpusSize documents")
    println(s"\t 词汇表大小：$actualVocabSize terms")
    println(s"\t 训练集大小：$actualNumTokens tokens")
    println(s"\t 转换特征耗时：$preprocessElapsed sec")
    println()
  }


  /**
    * LDA模型训练函数
    *
    * @param sc SparkContext
    * @param rdd  输入数据
    * @return (LDAModel, 词汇表)
    */
  def train(sc: SparkContext, rdd: RDD[String]): (LDAModel, RDD[String], RDD[(Long, Vector)], DataFrame) = {
    val k = config.k
    val maxIterations = config.maxIterations
    val vocabSize = config.vocabSize
    val algorithm = config.algorithm
    val alpha = config.alpha
    val beta = config.beta
    val checkpointDir = config.checkpointDir
    val checkpointInterval = config.checkpointInterval

    //将数据切分，转换为特征向量，生成词汇表，并计算数据总token数量
    val featureStart = System.nanoTime()
    val tokens = splitLine(sc, rdd, vocabSize)
    val (documents, vocabulary, actualNumTokens) = featureToVector(tokens, tokens, vocabSize)
    val vocabRDD = sc.parallelize(vocabulary)

    val actualCorpusSize = documents.count()
    val actualVocabSize = vocabulary.length
    val featureElapsed = (System.nanoTime() - featureStart) / 1e9

    featureInfo(actualCorpusSize, actualVocabSize, actualNumTokens, featureElapsed)

    val lda = new LDA()
    val optimizer = selectOptimizer(algorithm, actualCorpusSize)
    lda.setOptimizer(optimizer)
      .setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(alpha)
      .setTopicConcentration(beta)
      .setCheckpointInterval(checkpointInterval)

    if (checkpointDir.nonEmpty) {
      sc.setCheckpointDir(checkpointDir)
    }

    //训练LDA模型
    val trainStart = System.nanoTime()
    val ldaModel = lda.run(documents)
    val trainElapsed = (System.nanoTime() - trainStart) / 1e9

    trainInfo(documents, ldaModel, actualCorpusSize, trainElapsed)

    (ldaModel, vocabRDD, documents, tokens)
  }


  /**
    * 打印模型训练相关信息
    *
    * @param ldaModel         LDAModel
    * @param actualCorpusSize 实际语料大小
    * @param trainElapsed     训练耗时
    */
  def trainInfo(documents: RDD[(Long, Vector)], ldaModel: LDAModel, actualCorpusSize: Long, trainElapsed: Double) = {
    println(s"完成LDA模型训练！")
    println(s"\t训练时长：$trainElapsed sec")

    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
        val logPerplexity = distLDAModel.logPrior
        println(s"\t 训练数据平均对数似然度：$avgLogLikelihood")
        println(s"\t 训练数据对数困惑度：$logPerplexity")
        println()
      case localLDAModel: LocalLDAModel =>
        val avgLogLikelihood = localLDAModel.logLikelihood(documents) / actualCorpusSize.toDouble
        val logPerplexity = localLDAModel.logPerplexity(documents)
        println(s"\t 训练数据平均对数似然度：$avgLogLikelihood")
        println(s"\t 训练数据对数困惑度：$logPerplexity")
        println()
      case _ =>
    }
  }


  /**
    * 主题描述，包括主题下每个词以及词的权重
    *
    * @param ldaModel LDAModel
    * @param vocabArray 词汇表
    * @return 主题-词结果
    */
  def topicWords(ldaModel: LDAModel, vocabArray: Array[String]): Array[Array[(String, Double)]] = {
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
  }

  /**
    * 文档-主题分布结果
 *
    * @param ldaModel LDAModel
    * @param corpus  文档
    * @return “文档-主题分布”：(docID, topicDistributions)
    */
  def docTopics(ldaModel: LDAModel, corpus: RDD[(Long, Vector)]): RDD[(Long, Vector)] = {
    var topicDistributions: RDD[(Long, Vector)] = null
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        topicDistributions = distLDAModel.toLocal.topicDistributions(corpus)
//        topicDistributions = distLDAModel.topicDistributions
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.topicDistributions(corpus)
      case _ =>
    }

    topicDistributions
  }


  /**
    * 保存模型和tokens
    *
    * @param modelPath  模型保存路径
    * @param ldaModel LDAModel
    * @param tokens 词汇表：(index, word)
    */
  def saveModel(sc: SparkContext, modelPath: String, ldaModel: LDAModel, vocaRDD: RDD[String], tokens: DataFrame): Unit = {
    ldaModel match {
      case distModel: DistributedLDAModel =>
        distModel.toLocal.save(sc, modelPath + File.separator + "model")
      case localModel: LocalLDAModel =>
        localModel.save(sc, modelPath + File.separator + "model")
      case _ =>
        println("保存模型出错！")
    }
    tokens.write.parquet(modelPath + File.separator + "tokens")
    vocaRDD.saveAsTextFile(modelPath + File.separator + "vocab")
  }


  /**
    * 加载模型和tokens
    * @param sc SparkContext
    * @param modelPath  模型路径
    * @return (LDAModel, tokens)
    */
  def loadModel(sc: SparkContext, modelPath:String): (LDAModel, DataFrame, RDD[String]) = {
    val sqlContext = SQLContext.getOrCreate(sc)

    val ldaModel = LocalLDAModel.load(sc, modelPath + File.separator + "model")
    val tokens = sqlContext.read.parquet(modelPath + File.separator + "tokens")
    val vocabRDD = sc.textFile(modelPath + File.separator + "vocab")
    (ldaModel, tokens, vocabRDD)
  }

}

object LDAUtils {

  def apply(): LDAUtils = {
    LDAUtils("config/lda.properties")
  }

  def apply(propFile: String): LDAUtils = {
    val conf = LDAConfig(propFile)
    new LDAUtils(conf)
  }

  def apply(prop: Properties): LDAUtils = {
    val conf = LDAConfig(prop)
    new LDAUtils(conf)
  }

  def apply(conf: LDAConfig): LDAUtils = {
    new LDAUtils(conf)
  }
}
