package DataTypes

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Created by yhao on 2016/1/3.
 */
object LocalVectorDemo extends App {
    //创建一个稠密向量
    val dv1 = Vectors.dense(0.2, 0, 0.5, 4.2)

    //使用数组创建一个稀疏向量
    val sv1 = Vectors.sparse(5, Array(0, 1, 2, 4), Array(0.9, 2.2, 5.1, 0.31))

    //使用序列创建一个稀疏向量
    val sv2 = Vectors.sparse(3, Seq((0, 2.1), (2, 0.54)))

    println("=== 向量：" + "\n\t" + dv1 + "\n\t" + sv1 + "\n\t" + sv2)

    //稠密向量转化为稀疏向量
    val sv3 = dv1.toSparse

    //稀疏向量转化为稠密向量
    val dv2 = sv1.toDense
    val dv3 = sv2.toDense

    println("\n=== 向量：" + "\n\t" + sv3 + "\n\t" + dv2 + "\n\t" + dv3)
}
