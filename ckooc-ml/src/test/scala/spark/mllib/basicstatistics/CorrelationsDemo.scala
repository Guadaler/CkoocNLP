package spark.mllib.basicstatistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yhao on 2016/1/10.
 */
object CorrelationsDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val x = Seq(2.5, 4.0, 0.56)
  val y = Seq(5.0, 8.0, 1.12)

  val seriesX = sc.parallelize(x)
  val seriesY = sc.parallelize(y)

  val correlation = Statistics.corr(seriesX, seriesY, "pearson")
  println(correlation)

  val data = sc.parallelize(Seq(Vectors.dense(x.toArray), Vectors.dense(y.toArray)))

  val correlMatrix = Statistics.corr(data)
  println(correlMatrix.toString())

  sc.stop()
}
