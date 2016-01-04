package utils

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.linalg.{Vectors, SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Created by yhao on 2015/11/24.
 */
class MLUtils {

  //切分单词,限制保留词长度
  def tokenlizeLines(docDF: DataFrame): DataFrame = {
    val tokens = new RegexTokenizer()
      .setGaps(false)
      .setMinTokenLength(2)
      .setPattern("\\p{L}+")
      .setInputCol("text")
      .setOutputCol("words")
      .transform(docDF)
    tokens
  }

  //去除停用词
  def removeStopwords(tokens: DataFrame, stopwords: Array[String]): DataFrame = {
    val filteredTokens = new StopWordsRemover()
      .setStopWords(stopwords)
      .setCaseSensitive(false)
      .setInputCol("words")
      .setOutputCol("filtered")
      .transform(tokens)
    filteredTokens
  }

  //限制vocabSize个词频最高的词形成词汇表，将词频转化为特征向量
  def FeatureToVector(filteredTokens: DataFrame, vocabSize: Int): (CountVectorizerModel, RDD[(Long, Vector)]) = {
    val cvModel = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(filteredTokens)
    val countVectors = cvModel.transform(filteredTokens)
      .select("docId", "features")
      .map { case Row(docId: Long, countVector: Vector) => (docId, countVector) }
    (cvModel, countVectors)
  }
}

