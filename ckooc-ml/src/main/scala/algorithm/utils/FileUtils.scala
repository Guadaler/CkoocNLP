package algorithm.utils

import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * Created by yhao on 2016/3/12.
  */
class FileUtils {

  /**
    * 遍历目录得到该目录下的所有文件
    *
    * @param path 输入路径
    * @return 文件列表
    */
  def getFiles(path: String): Array[File] = {
    val rootFile = new File(path)
    val fileBuffer = new ArrayBuffer[File]()

    val files = rootFile.listFiles()
    for (file <- files) {
      if (file.isDirectory) {
        fileBuffer ++= getFiles(file.getAbsolutePath)
      } else if (file.isFile) {
        fileBuffer += file
      } else {
        println("文件（目录）不存在！")
      }
    }

    fileBuffer.toArray
  }

  /**
    * 读取文件内容
    *
    * @param file 输入文件
    * @return 文件内容，内各元素表示文件的一行
    */
  def readFile(file: String, encode: String = "utf-8"): Array[String] = {
    val lines = new ArrayBuffer[String]()
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(file), encode))

    var line = br.readLine()
    while (line != null) {
      lines.append(line)
      line = br.readLine()
    }

    lines.toArray
  }


  /**
    * 删除指定目录及其内的所有文件（目录）
    *
    * @param path 要删除的目录（文件）
    * @return 是否删除成功
    */
  def deleteDirectory(path: String): Boolean = {
    val rootFile = new File(path)
    var flag = false

    if (rootFile.isDirectory) {
      val files = rootFile.listFiles()
      for (file <- files) {
          deleteDirectory(file.getAbsolutePath)
          flag = true
      }
      rootFile.delete()
    } else if (rootFile.isFile) {
      rootFile.delete()
      flag = true
    }else {
      println("文件（目录）不存在！")
    }

    flag
  }


  /**
    * 转换文件编码(默认从GBK转换为UTF-8)
    *
    * @param inFile 输入文件
    * @param outFile  输出文件
    * @param srcEncode  输入编码
    * @param desEncode  输出编码
    */
  def convertEncode(inFile: String, outFile: String, srcEncode: String = "gbk", desEncode: String = "utf-8") = {
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(inFile), srcEncode))
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), desEncode))
    var line = br.readLine()
    while (line != null) {
      bw.write(line + "\n")
      line = br.readLine()
    }

    br.close()
    bw.close()
  }


  /**
    * 批量转换文件编码(默认从GBK转换为UTF-8)
    *
    * @param inPath 输入目录
    * @param outPath  输出目录
    * @param srcEncode  输入数据编码
    * @param desEncode  输出数据编码
    */
  def mutiConvertEncode(inPath: String, outPath: String, srcEncode: String = "gbk", desEncode: String = "utf-8") = {
    val files = getFiles(inPath)
    for (file <- files) {
      val inFile = file.getAbsolutePath
      var outFile = ""
      if (outPath.endsWith("\\/")) {
        outFile = outPath + file.getName
      } else {
        outFile = outPath + File.separator + file.getName
      }

      convertEncode(inFile, outFile, srcEncode, desEncode)
    }
  }


  /**
    * 按切分符切分并提取字段（单字段）添加到已有的文件，并按字典排序
    *
    * @param inFile 需要添加的文件
    * @param outFile  添加到的文件
    * @param flag 是否切分
    * @param sep  切分符
    * @param srcEncode  输入数据编码
    * @param desEncode  输出数据编码
    * @param index  要添加的字段index
    */
  def combineFiles(inFile: String, outFile: String, flag: Boolean, sep: String, srcEncode: String, desEncode: String, index: Int) = {
    val lines1 = readFile(inFile, srcEncode)
    val lines2 = readFile(outFile, desEncode)

    val basicUtils = new BasicUtils

    val fw = new FileWriter(outFile, false)

    var countNew = 0
    var tokens2 = lines2

    val tokensBuffer = new ArrayBuffer[String]()

    if (flag) {
      val tokens1 = lines1.map(line => line.split(sep)(index))

      for (token <- tokens1) {
        val index = basicUtils.binarySearch(token, tokens2, 0, tokens2.length - 1)
        if (index == -1) {tokensBuffer += token; countNew += 1}
      }
    } else {
      val tokensBuffer = tokens2.toBuffer

      for (token <- lines1) {
        val index = basicUtils.binarySearch(token, tokens2, 0, tokens2.length - 1)
        if (index == -1) {tokensBuffer += token; countNew += 1}
      }
    }

    tokens2 ++= tokensBuffer.toArray[String]
    Sorting.quickSort(tokens2)
    tokens2.foreach(token => fw.write(token + "\n"))

    println("新增新词：" + countNew + "个.")

    fw.close()
  }

}

object FileUtils {
  def main(args: Array[String]) {
    val fileUtils = new FileUtils

    val inFile = "C:/Users/yhao/Desktop/httpcws_dict.txt"
    val outFile = "C:/Users/yhao/Desktop/dictionary.dic"

    fileUtils.combineFiles(inFile, outFile, flag = true, "  ", "utf-8", "utf-8", 0)

  }
}
