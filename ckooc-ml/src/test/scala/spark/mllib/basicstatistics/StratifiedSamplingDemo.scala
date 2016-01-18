package spark.mllib.basicstatistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 分层采样示例
 * Created by yhao on 2016/1/10.
 */
object StratifiedSamplingDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  val data: RDD[(String, Double)] = sc.parallelize(Seq(("yhao", 5.0), ("abc", 3.2)))
  val fraction: Map[String, Double] = Map(("yhao", 0.4), ("abc", 0.6))

//  val approxSample: RDD[(String, Double)] = data.sampleByKey(withReplacement = false, fraction)
//  val exactSample: RDD[(String, Double)] = data.sampleByKeyExact(withReplacement = false, fraction)

  sc.stop()
}
