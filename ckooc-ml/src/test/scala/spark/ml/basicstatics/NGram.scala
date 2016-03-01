package spark.ml.basicstatics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yhao on 2016/3/1.
  */
object NGram {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("N-Gram").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val wordDataFrame = sqlContext.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
  }
}
