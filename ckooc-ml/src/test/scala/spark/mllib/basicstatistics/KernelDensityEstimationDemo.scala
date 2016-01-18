package spark.mllib.basicstatistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
  * Created by yhao on 2016/1/17.
  */
object KernelDensityEstimationDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  private val data: RDD[Double] = sc.parallelize(Array(0.3, 0.2, 0.5))

  private val kd: KernelDensity = new KernelDensity()
    .setSample(data)
    .setBandwidth(3.0)

  private val densities: Array[Double] = kd.estimate(Array(-1.0, 2.0, 5.0))
  println(densities.mkString(" "))
}
