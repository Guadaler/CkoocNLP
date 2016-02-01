package clustering.lda

import java.io.File
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by yhao on 2016/1/20.
  */
class LDAUtils(sc: SparkContext, sqlContext: SQLContext) {

  /**
    * 读取数据文件，切分，去除停用词
    *
    * @param paths  数据路径
    * @param vocabSize  词汇表大小
    * @param stopwordFile 停用词文件路径
    * @return (corpus, vocabArray, actualNumTokens)
    */
  def filter(
                  paths: Seq[String],
                  vocabSize: Int,
                  stopwordFile: String): DataFrame = {

    import sqlContext.implicits._

    val df = sc.textFile(paths.mkString(",")).toDF("docs")
    val customizedStopWords: Array[String] = if (stopwordFile.isEmpty) {
      Array.empty[String]   //用户自定义停用词
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

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover))

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
  def FeatureToVector(filteredTokens: DataFrame, trainTokens: DataFrame, vocabSize: Int): (RDD[(Long, Vector)], Array[String], Long) = {
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
    * 打印语料信息
    *
    * @param corpus 语料
    * @param vocabArray 词汇表
    * @param actualNumTokens  tokens个数
    */
  def corpusInfo(corpus: RDD[(Long, Vector)], vocabArray: Array[String], actualNumTokens: Long): Unit = {
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.length

    println(s"语料信息：")
    println(s"\t 训练集大小：$actualCorpusSize documents")
    println(s"\t 词汇表大小：$actualVocabSize terms")
    println(s"\t 总词汇数： $actualNumTokens tokens")
    println()
  }

  /**
    * 训练模型
    *
    * @param corpus 语料
    * @param k  topic个数
    * @param maxIterations  最大迭代次数
    * @param algorithm  算法（em或online）
    * @return LDAModel
    */
  def train(corpus: RDD[(Long, Vector)], k: Int = 20, maxIterations: Int = 20, algorithm: String = "em"): LDAModel = {
    val lda = new LDA()

    val actualCorpusSize = corpus.count()

    val optimizer = algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        "目前只支持em和online算法！")
    }

    lda.setOptimizer(optimizer)
      .setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(-1)
      .setTopicConcentration(-1)
      .setCheckpointInterval(10)

    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println("完成模型训练！")
    println(s"\t 耗时：$elapsed 秒")

    ldaModel
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
        topicDistributions = distLDAModel.topicDistributions
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.topicDistributions(corpus)
      case _ =>
    }

    topicDistributions
  }

  /**
    * 评估模型，评估标准：似然率和混乱率
    *
    * @param corpus  数据
    * @param ldaModel LDAModel
    * @return (logLikelihood, logPerplexity)
    */
  def evaluation(corpus: RDD[(Long, Vector)], ldaModel: LDAModel): (Double, Double) = {
    var logLikelihood = 0.0
    var logPerplexity = 0.0

    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        logLikelihood = distLDAModel.logLikelihood
        logPerplexity = distLDAModel.logPrior
      case localLDAModel: LocalLDAModel =>
        logLikelihood = localLDAModel.logLikelihood(corpus)
        logPerplexity = localLDAModel.logPerplexity(corpus)
      case _ =>
    }
    (logLikelihood, logPerplexity)
  }

  /**
    * 保存模型和词汇表
    *
    * @param modelPath  模型保存路径
    * @param ldaModel LDAModel
    * @param tokens 词汇表：(index, word)
    */
  def saveModel(modelPath: String, ldaModel: LDAModel, tokens: DataFrame): Unit = {
    ldaModel match {
      case distModel: DistributedLDAModel =>
        distModel.toLocal.save(sc, modelPath + File.separator + "model")
      case localModel: LocalLDAModel =>
        localModel.save(sc, modelPath + File.separator + "model")
      case _ =>
        println("保存模型出错！")
    }
    tokens.write.parquet(modelPath + File.separator + "df")
  }

  def loadModel(modelPath:String): (LDAModel, DataFrame) = {
    val ldaModel = LocalLDAModel.load(sc, modelPath + File.separator + "model")
    val tokens = sqlContext.read.parquet(modelPath + File.separator + "df")
    (ldaModel, tokens)
  }

  /**
    * 预测新文档的“文档-主题分布”
    *
    * @param ldaModel LDAModel
    * @param documents  文档
    * @return “文档-主题分布”：(docID, topicDistributions)
    */
  def predict(ldaModel: LDAModel, documents: RDD[(Long, Vector)]): RDD[(Long, Vector)] = {
    var topicDistributions: RDD[(Long, Vector)] = null
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        topicDistributions = distLDAModel.toLocal.topicDistributions(documents)
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.topicDistributions(documents)
      case _ =>
    }

    topicDistributions
  }
}
