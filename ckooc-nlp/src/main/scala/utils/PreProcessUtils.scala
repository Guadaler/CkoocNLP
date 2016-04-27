package utils

import java.io._
import java.util
import java.util.Properties

import com.hankcs.hanlp.HanLP
import conf.PreProcessConfig
import org.ansj.domain.Term
import org.ansj.splitWord.analysis.NlpAnalysis
import org.ansj.util.FilterModifWord
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}
import utils.chinese.ZHConverter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 数据预处理类，包含以下操作：
  * <p>&nbsp &nbsp &nbsp &nbsp基本清洗（繁简转换、全半角转换、去除无意义词）、分词、分句、分段、去除停用词、去除低频词</p>
  * <p>Created by yhao on 2016/3/12.</p>
  */
class PreProcessUtils (config: PreProcessConfig) extends Logging with Serializable {

  private val enExpr = "[A-Za-z]+".r
  //英文字符正则
  private val numExpr = "\\d+(\\.\\d+)?(\\/\\d+)?".r
  //数值正则，可以匹配203,2.23,2/12
  private val baseExpr = """[^\w-\s+\u4e00-\u9fa5]""".r //匹配英文字母、数字、中文汉字之外的字符

  private val zhConverter = ZHConverter.getInstance(1)


  /**
    * 对sc的textFile方法的封装，可以按指定的最小块进行切分读取
    * @param sc SparkContext
    * @param inPath 输入路径
    * @param minSize  最小块大小
    * @return RDD[String]
    */
  def getText(sc: SparkContext, inPath: String, minSize: Int = 32): RDD[String] = {
    val hadoopConf = sc.hadoopConfiguration
    val fs = new Path(inPath).getFileSystem(hadoopConf)
    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024)    //以MB为单位的数据大小
    val minPart = (len / minSize).toInt   //按minSize的分块数

    sc.textFile(inPath, minPart)
  }


  /**
    * 抽取指定字段的内容
    *
    * @param line 输入文本
    * @param sep  切分符
    * @param fieldsArray 要提取的字段，从0开始
    * @return 提取的字段数组
    */
  def contentExtract(line: String, cols: Int, sep: String, fieldsArray: Array[Int]): Array[String] = {
    val tokens = line.split(sep)
    val content = new Array[String](fieldsArray.length)
    if (tokens.length == cols) {
      if (tokens(0).length < 9) {
        for (i <- fieldsArray.indices) {
          content(i) = tokens(fieldsArray(i))
        }
      }
    }
    content
  }


  /**
    * 全角转半角
    *
    * @param line 输入数据
    * @return 转换为半角的数据
    */
  private def q2b(line: String): String = {
    zhConverter.convert(line)
  }


  /**
    * 基础清洗: 繁转简体、全角转半角、去除不可见字符
    *
    * @param line 输入数据
    * @return 清洗后的数据
    */
  private def baseClean(line: String): String = {
    var result = line
    if (config.f2j) {
      result = HanLP.convertToSimplifiedChinese(line)
    }
    if (config.q2b) {
      result = q2b(result) //全角转半角
    }
    result = baseExpr.replaceAllIn(result, "")
    StringUtils.trimToEmpty(result)
  }


  /**
    * 针对单行记录
    * 基础清理函数，包括去除不可见字符，数值替换，去英文字符
    *
    * @param delNum 替换数值
    * @param numToChar  数字的替换内容
    * @param delEn 是否去掉英文字符
    */
  def baseCleanFunc(line: String, delNum: Boolean, numToChar: String = "", delEn: Boolean): String = {
    var result = line.trim()
    result = baseClean(result)
    if (delNum) {
      result = numExpr.replaceAllIn(result, numToChar)
    }
    if (delEn) {
      result = enExpr.replaceAllIn(result, "")
    }
    result
  }


  /**
    * 分词,每行返回一个Seq[String]的分词结果
    *
    * @param text 原文本
    * @param delStopword 是否去停用词
    * @param minTermNum 分词后分词的个数，只保留大于指定个数的内容
    * @param minTermSize 单个term的长度,只保留大于指定长度的term
    */
  def wordSegment(text: String, delStopword: Boolean, minTermSize: Int, minTermNum: Int, stopwordArray: Array[String]): Option[Seq[String]] = {
    var arrayBuffer = ArrayBuffer[String]()
    if (text != null && text != "") {
      val tmp = new util.ArrayList[Term]()
      var result = NlpAnalysis.parse(text)
      if (delStopword) {
        val stopwordList = new util.ArrayList[String]()
        for (term <- stopwordArray) {
          stopwordList.add(term)
        }
        FilterModifWord.insertStopWords(stopwordList)
        result = FilterModifWord.modifResult(result)
      }
      tmp.addAll(result)

      for (i <- 0 until tmp.size()) {
        val term = tmp.get(i)
        var item = term.getName.trim()
        if (item.length() >= minTermSize) {
          arrayBuffer += item
        }
      }
      if (arrayBuffer.size >= minTermNum) {
        Some(arrayBuffer)
      } else {
        None
      }
    } else {
      None
    }
  }


  /**
    * 分段，对文本按指定的分隔符分段
    *
    * @param content  输入的一行数据
    * @param sep  分隔符
    * @return 每一段为一个元素的数组
    */
  def paragraphSegment(content: String, sep: String): Array[String] = {
    val result = new ArrayBuffer[String]()
    val paragraphs = content.split(sep)
    for (paragraph <- paragraphs) {
      val filterParagraph = paragraph.trim
      if (filterParagraph != null && filterParagraph != "") {
        result += filterParagraph
      }
    }

    result.toArray
  }


  /**
    * 获取低频词
    *
    * @param rareTermNum  词频阀值，低于此阀值的将会被过滤
    * @param wordRDD  词序列
    * @return 低频词数组
    */
  def getRareTerms(rareTermNum: Int, wordRDD: RDD[(Long, scala.Seq[String])]): Array[String] = {
    val wc = wordRDD.flatMap(words => words._2).map((_, 1)).reduceByKey(_ + _)
    val result = wc.filter(word => word._2 < rareTermNum).map(word => word._1)
    result.collect()
  }


  /**
    * 删除低频词
    *
    * @param words  输入词序列
    * @param rares  低频词数组
    * @return 删除低频词后的词
    */
  def delRareTerms(id: Long, words: Seq[String], rares: Array[String]): (Long, scala.Seq[String]) = {
    val result = new ArrayBuffer[String]()
    val wordsArray = words.toArray

    for (word <- wordsArray) {
      if (!rares.contains(word)) {
        result += word
      }
    }

    (id, result)
  }


  /**
    * 判断符号是否有意义
    *
    * @param ch 输入字符
    * @return 是否有意义，如果是则返回true
    */
  private def isMeaningful(ch: Char): Boolean = {
    var result = false
    val meaningfulMarks = Array('*', '-', 'X', '.','\\')
    if ((ch >= '一' && ch <= '龥') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || meaningfulMarks.contains(ch))
      result = true

    result
  }


  /**
    * 文本中出现明文的\r\n等转义符号
    *
    * @param content  一行文本
    * @return
    */
  private def dealWithSpecialCase(content: String): Array[String] = {
    val patterns = Array("\\\\r\\\\n", "\\\\n", "\\\\t", "[0-9]{8,}")
    val tmp = mutable.ArrayBuffer[String]()
    val result = mutable.ArrayBuffer[String]()
    result += content
    for (pat <- patterns) {
      tmp.clear()
      for (ele <- result) {
        val e = ele.trim()
        if (e != "") {
          tmp ++= e.replaceAll(pat, "|").split( """\|""")
        }
      }
      result.clear()
      result ++= tmp.clone()
    }

    result.toArray
  }



  /**
    * 分词主计算函数
    * 执行分词，返回一个Seq[String]类型的RDD数据，分词结果不用连接符连接
    *
    * @param sc SparkContext
    * @param rdd 输入的一行数据
    * @return 一个元素代表一条记录
    */
  def runPreProcess(sc: SparkContext, rdd: RDD[(Long, String)]): RDD[(Long, Seq[String])] = {
//    val splitWord = config.splitWord
    val delStopword = config.delStopword
    val minTermSize = config.minTermSize
    val minTermNum = config.minTermNum
    val rareTermNum = config.rareTermNum
    val delNum = config.delNum
    val delEn = config.delEn
    val toParagraphs = config.toParagraphs
    val paragraphSeparator = config.paragraphSeparator
    val numToChar = config.numToChar

    val contentRDD = rdd

    //是否分段
    /*if (toParagraphs) {
      contentRDD = contentRDD.flatMap(line => paragraphSegment(line, paragraphSeparator))
    }*/

    val stopwordArray = sc.textFile(config.stopwordPath).collect()
    val broadStopword = sc.broadcast(stopwordArray)

    //清洗数据，分词，去除停用词
    var resultRDD = contentRDD.map(str => (str._1, baseCleanFunc(str._2, delNum, numToChar, delEn))).map{line =>
      (line._1, wordSegment(line._2, delStopword, minTermSize, minTermNum, broadStopword.value))
    }.filter(_._2.nonEmpty).map(line => (line._1, line._2.get))

    broadStopword.unpersist()

    //去除低频词
    if (config.delRareTerm) {
      val rareArray = getRareTerms(rareTermNum, resultRDD)
      resultRDD = resultRDD.map(words => delRareTerms(words._1, words._2, rareArray))
    }

    resultRDD
  }
}

object PreProcessUtils extends Logging {

  def apply(): PreProcessUtils = {
    PreProcessUtils("config/preprocess.properties")
  }

  def apply(confFile: String): PreProcessUtils = {
    val config = PreProcessConfig(confFile)
    new PreProcessUtils(config)
  }

  def apply(prop: Properties): PreProcessUtils = {
    val config = PreProcessConfig(prop)
    new PreProcessUtils(config)
  }

  def apply(conf: PreProcessConfig): PreProcessUtils = {
    new PreProcessUtils(conf)
  }
}
