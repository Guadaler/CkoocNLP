package DataTypes

import org.apache.spark.mllib.linalg.Matrices

/**
 * Created by yhao on 2016/1/4.
 */
class LocalMatrixDemo extends App {
  //创建一个3行2列的稠密型的local matrix
  val dm = Matrices.dense(3, 2, Array(2.4, 3.1, 0.54, 1.1, 10.2, 2.9))

  //创建一个稀疏型的local matrix
  val sm = Matrices.sparse(3, 2, Array(0, 2, 3), Array(4, 2, 1), Array(6, 2, 9))

  println("dense: ")
}
