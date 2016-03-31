package algorithm.utils

/**
  * 各种距离计算
  * Created by yhao on 2016/3/21.
  */
class DistanceUtils {

  /**
    * 计算两个向量的曼哈顿距离(当P=1时的闵可夫斯基距离)
    * @param v1 向量v1
    * @param v2 向量v2
    * @return 曼哈顿距离
    */
  def manhattanDistance(v1: Vector[Double], v2: Vector[Double]): Double = {
    val distance = minkowskiDistance(v1, v2, 1)
    distance
  }


  /**
    * 计算两个向量的欧氏距离(当P=2时的闵可夫斯基距离)
    *
    * @param v1 向量v1
    * @param v2 向量v2
    * @return 欧氏距离
    */
  def euclideanDistance(v1: Vector[Double], v2: Vector[Double]): Double = {
    val distance = minkowskiDistance(v1, v2, 2)
    distance
  }


  /**
    * 计算两个向量的闵可夫斯基距离
    *
    * @param v1 向量v1
    * @param v2 向量v2
    * @param p  闵可夫斯基公式P值
    * @return 闵可夫斯基距离
    */
  def minkowskiDistance(v1: Vector[Double], v2: Vector[Double], p: Int): Double = {
    var distance = -1.0
    if (v1.length == v2.length) {
      val vector = v1.zip(v2)
      val sum = vector.map{case(x1, x2) => math.pow(x1 + x2, p)}.sum
      distance = math.pow(sum, 1d/p)
    } else {
      println("ERROR! 两向量长度不同！")
      sys.exit()
    }

    distance
  }


  /**
    * 计算两个向量的余弦距离
    *
    * @param v1 向量v1
    * @param v2 向量v2
    * @return 余弦距离
    */
  def cosineDistance(v1: Vector[Double], v2: Vector[Double]): Double = {
    var distance = -1.0
    if (v1.length == v2.length) {
      val vector = v1.zip(v2)
      val x1x2 = vector.map{case(x1, x2) => x1 * x2}.sum
      var x1sum = v1.map(x1 => math.pow(x1, 2)).sum
      x1sum = math.pow(x1sum, 1.0/2)
      var x2sum = v2.map(x2 => math.pow(x2, 2)).sum
      x2sum = math.pow(x2sum, 1.0/2)

      distance = x1x2 / (x1sum * x2sum)
    } else {
      println("ERROR! 两向量长度不同！")
      sys.exit()
    }

    distance
  }
}
