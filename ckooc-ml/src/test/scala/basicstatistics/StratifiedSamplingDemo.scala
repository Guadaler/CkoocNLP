package basicstatistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 分层采样示例
 * Created by yhao on 2016/1/10.
 */
object StratifiedSamplingDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("Correlations").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val data = sc.parallelize(Seq(("yhao", 5.0), ("abc", 3.2)))
  val fraction = Map(("yhao", 0.4), ("abc", 0.6))

//  val approxSample = data.sampleByKey(withReplacement = false, fraction)
//  val exactSample = data.sampleByKeyExact(withReplacement = false, fraction)

  sc.stop()
}
