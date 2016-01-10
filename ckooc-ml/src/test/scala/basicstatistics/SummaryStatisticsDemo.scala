package basicstatistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.mllib.linalg.{Vectors, Matrices}
import org.apache.spark.mllib.stat.Statistics

/**
 * 统计向量组的相关信息，包括最小值、最大值、平均值、方差等
 * Created by yhao on 2016/1/10.
 */
object SummaryStatisticsDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)


  val dv1 = Vectors.dense(0.2, 0, 0.5, 4.2)
  val sv1 = Vectors.sparse(4, Array(0, 1, 2, 3), Array(0.9, 2.2, 5.1, 0.31))

  val conf = new SparkConf().setAppName("Summary Statistics").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val vectors = sc.parallelize(Seq(dv1, sv1))
  val summary = Statistics.colStats(vectors)

  println()
  println(summary.min)
  println(summary.variance)
  println(summary.numNonzeros)
}
