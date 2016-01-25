package spark.mllib.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by yhao on 2016/1/25.
  */
object SVMDemo {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "ckooc-ml/data/classification/sample_libsvm_data.txt")

    //将数据按60%训练，40%测试进行划分
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1)

    //训练模型
    val numIterations: Int = 100
    val model: SVMModel = SVMWithSGD.train(training, numIterations)

    //明确阀值
    model.clearThreshold()

    //计算测试集上的每一行得分
    val scoreAndLabels: RDD[(Double, Double)] = test.map{point =>
      val score: Double = model.predict(point.features)
      (score, point.label)
    }

    //获取评估矩阵
    val metrics: BinaryClassificationMetrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC: Double = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    //保存模型
    model.save(sc, "G:/svmModel")

    //加载模型
    val sameModel: SVMModel = SVMModel.load(sc, "G:/svmModel")

  }

}
