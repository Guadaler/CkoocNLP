package conf

import java.io.{FileInputStream, InputStreamReader, BufferedReader}

import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.Logging

import java.util.Properties

/**
  * Created by yhao on 2016/3/18.
  */
case class LDAConfig(k: Int, maxIterations: Int, vocabSize: Int, algorithm: String, alpha: Double, beta: Double,
                     checkpointDir: String, checkpointInterval: Int)

object LDAConfig extends Logging {

  val PARAMS = List("k", "maxIterations", "vocabSize", "algorithm", "alpha",
    "beta", "checkpointDir", "checkpointInterval")

  /**
    * 读取配置项
    * @param prop 配置
    * @return LDAConfig
    */
  def apply(prop: Properties): LDAConfig = {
    checkParams(prop)
    printParams(prop)

    val k = NumberUtils.toInt(prop.getProperty("k"), 10)
    val maxIterations = NumberUtils.toInt(prop.getProperty("maxIterations"), 20)
    val vocabSize = NumberUtils.toInt(prop.getProperty("vocabSize"), 10000)
    val algorithm = prop.getProperty("algorithm", "em")
    val alpha = NumberUtils.toDouble(prop.getProperty("alpha"), -1)
    val beta = NumberUtils.toDouble(prop.getProperty("beta"), -1)
    val checkpointDir = prop.getProperty("checkpointDir", "")
    val checkpointInterval = NumberUtils.toInt(prop.getProperty("checkpointInterval"), 10)

    new LDAConfig(k, maxIterations, vocabSize, algorithm, alpha, beta, checkpointDir, checkpointInterval)
  }

  /**
    * 读取配置文件
    *
    * @param propFile 配置文件路径
    * @return LDAConfig
    */
  def apply(propFile: String): LDAConfig = {
    val prop = new Properties()
    try {
      prop.load(new BufferedReader(new InputStreamReader(new FileInputStream(propFile), "utf-8")))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    LDAConfig(prop)
  }

  /**
    * 从key=value数组里初始化配置类
    *
    * @param kvs key=value数组
    * @return Config
    */
  def apply(kvs: Array[String]): LDAConfig = {
    val prop = new Properties
    kvs.foreach { kv => {
      val temp = kv.split("=")
      val key = temp(0)
      val value = temp(1)
      prop.setProperty(key, value)
    }
    }

    LDAConfig(prop)
  }


  /**
    * 打印参数
    *
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
    *
    * @param prop 配置
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

}
