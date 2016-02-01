package spark.mllib.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by yhao on 2016/1/25.
  */
object BinaryClassificationDemo {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //svm文件格式： label featureIndex1:value1 featureIndex2:value2 ...
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "ckooc-ml/data/classificationAndRegression/sample_libsvm_data.txt")

    //将数据按60%训练，40%测试进行划分
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1)

    //正则化类型
    val regType = "L1"
    val updater = regType match {
      case "L1" => new L1Updater()
      case "L2" => new SquaredL2Updater()
    }

    //训练模型(方法一)
    val algorithmType = "LR"
    val model = algorithmType match {
      case "SVM" =>
        val algorithm = new SVMWithSGD()
        algorithm.optimizer
          .setNumIterations(30)
          .setUpdater(updater)
        algorithm.run(training).clearThreshold()
      case "LR" =>
        val algorithm = new LogisticRegressionWithSGD()
        algorithm.optimizer
          .setNumIterations(30)
          .setUpdater(updater)
        algorithm.run(training).clearThreshold()
    }

    //训练模型(方法二)
//    val model: SVMModel = SVMWithSGD.train(training, 100)
//    val model: LogisticRegressionModel = LogisticRegressionWithSGD.train(training, 100)

    //计算测试集上的每一行得分，得分为负则认为label=0，否则可认为label=1
    val scoreAndLabels: RDD[(Double, Double)] = test.map{point =>
      val score: Double = model.predict(point.features)
      (score, point.label)
    }

    //输出得分和标签，进行对比
    scoreAndLabels.take(10).foreach{case(score, label) =>
      println("Score: " + score + ", Label: " + label)
    }

    //获取评估矩阵
    val metrics: BinaryClassificationMetrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC: Double = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    //保存模型
//    model.save(sc, "G:/svmModel")

    //加载模型
//    val sameModel: SVMModel = SVMModel.load(sc, "G:/svmModel")

    sc.stop()
  }
}

