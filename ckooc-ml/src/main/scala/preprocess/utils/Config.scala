package preprocess.utils



import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties

import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.Logging

/**
 * NLP数据预处理配置类
 */
case class Config(f2j: Boolean, q2b: Boolean, delNum: Boolean, numToChar: String, delEn: Boolean,
  delStopword: Boolean, minLineLen: Int, toSentence: Boolean,
  splitWord: Boolean, oneGram: Boolean, minTermSize: Int, minTermNum: Int,
  delRareTerm: Boolean, rareTermNum: Int, toParagraphs: Boolean, paragraphSeparator: String)

object Config extends Logging {

  /**
    * 参数列表
    */
  val PARAMS = List("f2j", "q2b", "delNum", "delEn", "delStopword", "minLineLen",
    "toSentence", "splitWord", "minTermSize", "minTermNum", "delRareTerm",
    "rareTermNum", "toParagraphs", "paragraphSeparator", "numToChar", "oneGram")

  /**
    * 根据Properties配置文件返回一个Config对象
    *
    * @param prop 参数配置
    * @return Config
    */
  def apply(prop: Properties): Config = {
    checkParams(prop)
    printParams(prop)
    val f2j = BooleanUtils.toBoolean(if (prop.getProperty("f2j") == null) "true" else prop.getProperty("f2j"))
    val q2b = BooleanUtils.toBoolean(if (prop.getProperty("q2b") == null) "true" else prop.getProperty("q2b"))
    val delNum = BooleanUtils.toBoolean(if (prop.getProperty("delNum") == null) "false" else prop.getProperty("delNum"))
    val delEn = BooleanUtils.toBoolean(if (prop.getProperty("delEn") == null) "false" else prop.getProperty("delEn"))
    val delStopword = BooleanUtils.toBoolean(if (prop.getProperty("delStopword") == null) "false" else prop.getProperty("delStopword"))
    val minLineLen = NumberUtils.toInt(prop.getProperty("minLineLen"), 20)
    val toSentence = BooleanUtils.toBoolean(if (prop.getProperty("toSentence") == null) "false" else prop.getProperty("toSentence"))
    val splitWord = BooleanUtils.toBoolean(if (prop.getProperty("splitWord") == null) "false" else prop.getProperty("splitWord"))
    val minTermSize = NumberUtils.toInt(prop.getProperty("minTermSize"), 1)
    val minTermNum = NumberUtils.toInt(prop.getProperty("minTermNum"), 10)
    val delRareTerm = BooleanUtils.toBoolean(if (prop.getProperty("delRareTerm") == null) "false" else prop.getProperty("delRareTerm"))
    val rareTermNum = NumberUtils.toInt(prop.getProperty("rareTermNum"), 1)
    val toParagraphs = BooleanUtils.toBoolean(if (prop.getProperty("toParagraphs") == null) "false" else prop.getProperty("toParagraphs"))
    val paragraphSeparator = if (prop.getProperty("paragraphSeparator") == null) "		" else prop.getProperty("paragraphSeparator").replaceAll("<|>", "")
    val numToChar = prop.getProperty("numToChar", "")
    val oneGram = BooleanUtils.toBoolean(if (prop.getProperty("oneGram") == null) "false" else prop.getProperty("oneGram"))
    new Config(f2j, q2b, delNum, numToChar, delEn, delStopword, minLineLen, toSentence, splitWord, oneGram, minTermSize, minTermNum, delRareTerm, rareTermNum, toParagraphs, paragraphSeparator)
  }

  /**
    * 从prop文件初始化配置文件
    * @param propFile 配置文件路径
    * @return Config
    */
  def apply(propFile: String): Config = {
    val prop = new Properties
    try {
      prop.load(new BufferedReader(new InputStreamReader(new FileInputStream(propFile), "UTF-8")))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    Config(prop)
  }

  /**
    * 从key=value数组里初始化配置类
    * @param kvs  key=value数组
    * @return Config
    */
  def apply(kvs: Array[String]): Config = {
    val prop = new Properties
    kvs.foreach { kv =>
      {
        val temp = kv.split("=")
        val key = temp(0)
        val value = temp(1)
        prop.setProperty(key, value)
      }
    }
    Config(prop)
  }

  /**
    * 打印参数
    * @param prop 配置项
    */
  def printParams(prop: Properties) {
    val propSet = prop.entrySet().iterator()
    while (propSet.hasNext) {
      val entry = propSet.next()
      log.info(s"params: ${entry.getKey}=${entry.getValue}")
    }
  }

  /**
    * 检查参数
    * @param prop 配置项
    */
  def checkParams(prop: Properties) {
    val keyItr = prop.keySet().iterator()
    while (keyItr.hasNext) {
      val key = keyItr.next()
      if (!PARAMS.contains(key)) {
        throw new Exception(s"unknown param: $key")
      }
    }
  }

  def main(args: Array[String]) {
    val datap = PreProcessUtils()
    val config = Config("config/ml-config.properties")
  }
}

