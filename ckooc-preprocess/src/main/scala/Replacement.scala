import java.io._

/**
  * Created by yhao on 2016/1/12.
  */
object Replacement {

  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)

//    val input = "G:\\test\\sample.txt"
//    val output = "G:\\test\\result.txt"

    val br = new BufferedReader(new InputStreamReader(new FileInputStream(input)))
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output, true)))

    var line = br.readLine()
    var replacedLine = ""
    var temp = ""
    while (line != null) {
//      temp = replaceSegChar(line, "\\!\\@\\#\\$\\%", "\u00EF")
      replacedLine = replaceURL(line, "\u00EF")
      bw.write(replacedLine + "\n")
      line = br.readLine()
    }

    bw.close()
  }

  /**
    * 替换切分符
    * @param line 待切分的行
    * @param inSeg  原切分符
    * @param outSeg 目标切分符
    * @return 替换后的行
    */
  def replaceSegChar(line: String, inSeg: String, outSeg: String): String = {
    val replacedLine = line.replaceAll(inSeg, outSeg)
    replacedLine
  }

  /**
    * 将url替换为url中包含的id
    * @param line 输入的行
    * @param inSeg  切分符
    * @return 替换后的行
    */
  def replaceURL(line: String, inSeg: String): String = {
    val tokens = line.split(inSeg)
    val url = tokens(0)
    if (url.contains("url")) {
      tokens(0) = url.substring(url.lastIndexOf("/") + 1, url.lastIndexOf("."))
    }
    tokens.mkString(inSeg)
  }
}
