package utils

import java.io._

import scala.collection.mutable.ArrayBuffer

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
      bw.write(line)
      line = br.readLine()
    }

    br.close()
    bw.close()
  }

}

object FileUtils {
  def main(args: Array[String]) {
    val fileUtils = new FileUtils

    /*val files = fileUtils.getFiles("G:\\data\\分词标准测试数据")

    for (file <- files) {
      println(file.getName)
    }*/

    fileUtils.deleteDirectory("G:\\分词标准测试数据")
  }
}
