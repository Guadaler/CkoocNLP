package spark.mllib.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.optimization.{L1Updater, SimpleUpdater, SquaredL2Updater}
import org.apache.spark.mllib.regression.{LabeledPoint, LassoWithSGD, LinearRegressionWithSGD, RidgeRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yhao on 2016/2/1.
  */
object LinearRegressionDemo {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //加载数据
    val parsedData: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "ckooc-ml/data/classificationAndRegression/sample_linear_regression_data.txt")

    //将数据按60%训练，40%测试进行划分
    val splits: Array[RDD[LabeledPoint]] = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1)

    //正则化类型
    val regType = "L1"
    val updater = regType match {
      case "NONE" => new SimpleUpdater()
      case "L1" => new L1Updater()
      case "L2" => new SquaredL2Updater()
    }

    //训练模型
    val algorithmType = "Lasso"
    val model = algorithmType match {
      case "Linear" =>  //线性回归模型训练
        val algorithm = new LinearRegressionWithSGD()
        algorithm.optimizer
          .setNumIterations(30)
          .setUpdater(updater)
        algorithm.run(training)
      case "Ridge" =>   //岭回归模型训练
        val algorithm = new RidgeRegressionWithSGD()
        algorithm.optimizer
          .setNumIterations(30)
          .setUpdater(updater)
        algorithm.run(training)
      case "Lasso" =>   //拉索回归模型训练
        val algorithm = new LassoWithSGD()
        algorithm.optimizer
          .setNumIterations(30)
          .setUpdater(updater)
        algorithm.run(training)
    }

    //使用训练模型进行预测
    val labelAndPrediction = test.map {point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //输出得分和标签，进行对比
    labelAndPrediction.take(10).foreach{case(label, prediction) =>
      println("Label: " + label + ", Prediction: " + prediction)
    }

    //计算均方误差来估计拟合度
    val MSE = labelAndPrediction.map{case(v, p) => math.pow(v - p, 2)}.mean()
    println("training Mean Squared Error = " + MSE)

    //保存模型
    model.save(sc, "G:/svmModel")

    sc.stop()
  }
}
