package algorithm.utils

/**
  * Created by yhao on 2016/3/28.
  */
class BasicUtils {

  /**
    * 二分查找，返回在词典中的Index
    *
    * @param word 待查找的词
    * @param dictionary 已排序的词典
    * @param from 开始位置
    * @param to 结束位置
    * @return Index
    */
  def binarySearch(word: String , dictionary : Array[String], from: Int = 0, to: Int) : Int = {
    var L = from
    var R = to - 1
    var mid : Int = 0
    while (L < R) {
      mid = (L + R) / 2
      if (word > dictionary(mid))
        L = mid + 1
      else
        R = mid
    }
    if (word != dictionary(L)){
//      println("NOT FIND WORD" + word)
      return -1
    }
    L
  }
}
