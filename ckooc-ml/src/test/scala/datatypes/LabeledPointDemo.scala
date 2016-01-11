package datatypes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by yhao on 2016/1/3.
 */
object LabeledPointDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("LabeledPoint").setMaster("local[2]")
  val sc = new SparkContext(conf)

  //创建一个标签为0的向量来代表正面
  val pos = LabeledPoint(0.0, Vectors.dense(0.2, 0, 0.5, 4.2))

  //创建一个标签为1的向量来代表负面
  val neg = LabeledPoint(1.0, Vectors.sparse(5, Array(0, 1, 2, 4), Array(0.9, 2.2, 5.1, 0.31)))

  println("=== 标签：" + "\n\t" + pos + "\n\t" + neg)

  //加载labeled point
  val example = MLUtils.loadLabeledPoints(sc, "ckooc-ml/data/datatype/sample_libsvm_data.txt")

  sc.stop()
}
