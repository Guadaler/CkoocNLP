import java.io._

/**
  * Created by yhao on 2016/1/13.
  */
object ContExtraction {
  def main(args: Array[String]) {
//    val input = "G:\\data\\baike\\test.txt"
//    val output = "G:\\data\\baike\\result.txt"

    val input = args(0)
    val output = args(1)

    val br = new BufferedReader(new InputStreamReader(new FileInputStream(input)))
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output, true)))

    var line = br.readLine()
    var content = ""
    while (line != null) {
      content = baikeExtraction(line)
      if (content != null) bw.write(content + "\n")
      line = br.readLine()
    }

    bw.close()
  }

  /**
    * 提取百科的内容，百科数据包括：ID、title、keywords、desc、tags、text
    * @param line 输入的一行数据
    * @param seg  切分符
    * @return 百科内容
    */
  def baikeExtraction(line: String, seg: String = "\u00EF"): String = {
    val tokens = line.split(seg)
    val content = tokens.last
    var result = ""
    if (content.length > 4) {
      result = content.substring(5, content.length)
    } else {
      result = content
    }

    result
  }
}
