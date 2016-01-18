package spark.mllib.basicstatistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.mllib.random.RandomRDDs._

/**
  * Created by yhao on 2016/1/17.
  */
object RandomDataGenerationDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  //创建一个包含100W从标准正态分布N(0, 1)获取的独立同分布数值，均匀分布于10个分区
  val u: RDD[Double] = normalRDD(sc, 1000000L, 10)
  println(u.take(5).mkString(" "))

  //将数值从服从N(0, 1)转化为服从N(1, 4)
  val v = u.map(x => 1.0 + 2.0 * x)
  println(v.take(5).mkString(" "))
}
