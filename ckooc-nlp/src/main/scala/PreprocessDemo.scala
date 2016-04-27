import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import utils.PreProcessUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yhao on 2016/4/27.
  */
object PreprocessDemo {
  def main(args: Array[String]) {
    //设置log等级为WARN
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("DataPreProcess").setMaster("local")
    val sc = new SparkContext(conf)

    val args = Array("data/sample_data2.txt", "data/preprocess_result.txt", "zhihu")
    //    val args = Array("data/baike/baike_sample_data.txt", "data/preprocess_baike_result.txt", "baike")

    val preUtils = PreProcessUtils("config/preprocess.properties")

    val inFile = args(0)
    val outFile = args(1)
    val dataType = args(2)

    val splitSize = 48
    val sep = "\u00EF"

    var fieldSize = 0
    val fieldsBuffer = new ArrayBuffer[Int]()

    dataType match {
      case "zhihu" =>
        //提取指定字段的数据(id, title, content)
        fieldSize = 14
        fieldsBuffer +=(0, 6, 13)

      case "baike" =>
        //提取指定字段的数据(id, title, content)
        fieldSize = 6
        fieldsBuffer +=(0, 4, 5)

      case _ =>
        println("数据类型不支持！")
        sys.exit()
    }

    val fieldsArray = fieldsBuffer.toArray
    val extractRDD = preUtils.getText(sc, inFile, splitSize).map(line => preUtils.contentExtract(line, fieldSize, sep, fieldsArray)).filter(line => line(2) != null)

    //合并字段内容(title+content)
    val textRDD = extractRDD.map(tokens => (tokens(0).toLong, tokens(1) + tokens(2)))

    //分词等预处理，得到分词后的结果
    val splitedRDD = preUtils.runPreProcess(sc, textRDD)

    //--本地测试使用：写入本地文件
    val result = splitedRDD.map(words => words._1 + "|" + words._2.mkString(" ")).collect()
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))
    for (line <- result) {
      bw.write(line + "\n")
    }
    bw.close()

    //--集群使用：写入HDFS指定路径
    /*val result = splitedRDD.map(words => words._1 + "|" + words._2.mkString(" "))
    result.saveAsTextFile(outFile)*/

    sc.stop()
  }
}
