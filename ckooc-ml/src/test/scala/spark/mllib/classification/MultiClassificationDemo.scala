package spark.mllib.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yhao on 2016/2/2.
  */
object MultiClassificationDemo {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //svm文件格式： label featureIndex1:value1 featureIndex2:value2 ...
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "ckooc-ml/data/classificationAndRegression/" +
      "sample_multiclass_classification_data.txt")

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

    val algorithm = new LogisticRegressionWithLBFGS().setNumClasses(10)
    algorithm.optimizer
      .setNumIterations(30)
      .setUpdater(updater)
    val model = algorithm.run(training)

    //计算测试集上的行得分
    val labelAndPrediction = test.map{case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (label, prediction)
    }

    labelAndPrediction.take(10).foreach{case (label, prediction) =>
      println("Label: " + label + ", Prediction: " + prediction)
    }
    println()

    //矩阵对象实例
    val metrics: MulticlassMetrics = new MulticlassMetrics(labelAndPrediction)

    //混乱矩阵
    println("Confusion matrix:")
    println(metrics.confusionMatrix)
    println()

    //统计
    val precision: Double = metrics.precision   //准确率
    val recall: Double = metrics.recall         //召回率
    val f1Score: Double = metrics.fMeasure      //F1值

    println("Summary Statistics")
    println(s"Precision = $precision")
    println(s"Recall = $recall")
    println(s"F1 Score = $f1Score")
    println()

    sc.stop()
  }
}
