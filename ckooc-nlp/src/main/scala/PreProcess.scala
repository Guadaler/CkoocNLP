import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.io.Source

/**
 * Created by yhao on 2015/12/24.
 */
class PreProcess {

  def removeStopword(lines: Array[String]): Array[String] = {
    val stopwordInput = "ckooc-nlp/data/stopword.txt"
    this.removeStopword(lines, stopwordInput)
  }

  def removeStopword(lines: Array[String], stopwordInput: String): Array[String] = {
    this.removeStopword(lines, stopwordInput, "utf-8")
  }

  def removeStopword(lines: Array[String], stopwordInput: String, encoding: String): Array[String] = {

    val stopwords = Source.fromFile(stopwordInput, encoding).getLines()

    val tokened = new Array[String](lines.length)

    for (i <- 0 until lines.length) {
      val tokens = lines(i).split(" ")
      val tokenedArray = tokens.filter(s => !stopwords.contains(s))
      tokened(i) = tokenedArray.mkString(" ")
    }
    tokened
  }

  def lineToWC(sc:SparkContext, lines: Array[String]): Array[String] = {
    val filterLines = new Array[String](lines.length)
    for (i <- 0 until lines.length) {
      val tokensRDD = sc.parallelize(lines(i).split(" "))
      val reducedTokens = tokensRDD.map(x => (x, 1)).reduceByKey(_ + _).collect()
      var tmpStr = ""
      reducedTokens.foreach(x => {
        tmpStr += x._1 + " " + x._2 + " "
      })
      filterLines(i) = tmpStr
    }
    filterLines
  }
}
