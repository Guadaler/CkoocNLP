package datatype

import org.apache.spark.mllib.linalg.Matrices

/**
 * 本地矩阵示例
 * Created by yhao on 2016/1/4.
 */
object LocalMatrixDemo extends App {
  //创建一个3行2列的稠密型的local matrix
  val dm = Matrices.dense(3, 2, Array(2.4, 3.1, 0.54, 1.1, 10.2, 2.9))

  //创建一个稀疏型的local matrix
  val sm = Matrices.sparse(6, 4, Array(0, 2, 4, 6, 7), Array(1, 3, 0, 4, 2, 5, 2), Array(2, 3, 1, 9, 7, 7, 6))

  println("dense: " + dm.toArray.mkString(" "))
  println("sparse: " + sm.toArray.mkString(" "))
}
