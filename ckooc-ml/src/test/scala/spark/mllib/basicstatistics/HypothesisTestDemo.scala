package spark.mllib.basicstatistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.{KolmogorovSmirnovTestResult, ChiSqTestResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yhao on 2016/1/16.
  */
object HypothesisTestDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  //由事件频率组成的向量
  val vec: Vector = Vectors.dense(0.2, 0.1, 0.5, 0.2)
  //计算拟合度。如果没有提供第二个向量作为参数，将测试其均匀分布。
  val goodnessOfFitTestResult: ChiSqTestResult = Statistics.chiSqTest(vec)
  println(goodnessOfFitTestResult)
  //包含使用方法、自由度、检验统计量、P值、零值假设的总结。


  //可能性矩阵
  val mat: Matrix = Matrices.dense(2, 4, Array(0.2, 0.1, 0.5, 0.2, 0.05, 0.2, 0.6, 0.15))
  //在可能性矩阵上进行Pearson独立性测试
  val independenceTestResult: ChiSqTestResult = Statistics.chiSqTest(mat)
  println("\n" + independenceTestResult)


  val pos: LabeledPoint = LabeledPoint(0.0, Vectors.dense(0.2, 0, 0.5, 4.2))
  val neg: LabeledPoint = LabeledPoint(1.0, Vectors.sparse(5, Array(0, 1, 2, 4), Array(0.9, 2.2, 5.1, 0.31)))

  val obs = sc.parallelize(Seq(pos, neg))
  //列联表是由(feature, label)键值对组成的行组成，用户进行独立性测试。
  // 返回一个在对应标签上对每个特征的featureTestResults。
  val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
  var i: Int = 1
  featureTestResults.foreach { result =>
    println(s"\nColumn $i:\n$result\n")
    i += 1
  }


  private val data: RDD[Double] = sc.parallelize(Array(0.3, 0.2, 0.5))
  //运行KS测试的1-sample和2-sided
  private val testResult: KolmogorovSmirnovTestResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
  println(testResult)


  //使用自定义累积分布函数计算KS
  val myCDF: Double => Double = x => Math.exp(-x)
  private val testResult2: KolmogorovSmirnovTestResult = Statistics.kolmogorovSmirnovTest(data, myCDF)
  println(testResult2)
}