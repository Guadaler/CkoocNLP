package preprocess

import scala.io.Source

/**
 * Created by yhao on 2015/12/24.
 */
class LineToWC {
  def main(args: Array[String]) {
    val input = ""
    val lines = Source.fromFile(input, "utf-8").getLines()

    var line = ""
    while (lines.hasNext) {
      line = lines.next()

    }

  }
}
