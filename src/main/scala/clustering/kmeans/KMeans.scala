/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package clustering.kmeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, mllib}


object KMeans {

  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"
  
  
  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import InitializationMode._

  /**
   * 训练模型
   * @param input 输入文件
   * @param k 聚类个数
   * @param numIterations 最大迭代次数
   * @param initializationMode  初始化模型（Parallel或者Random）
   * @return  KMeans模型
   */
  def train(input: RDD[String], k: Int, numIterations: Int, runs: Int = 1, initializationMode: InitializationMode = Parallel): KMeansModel = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val examples = input.map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.cache()

    val numExamples = examples.count()

    println(s"实例个数： $numExamples.")

    val initMode = initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    new mllib.clustering.KMeans()
      .setInitializationMode(initMode)
      .setK(k)
      .setMaxIterations(numIterations)
      .setRuns(runs)
      .run(examples)
  }

  /**
   * 打印聚类中心以及每条数据的类簇
   * @param input 输入数据
   * @param model KMeans模型
   */
  def print(input: RDD[String], model: KMeansModel): Unit = {
    val examples = input.map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.cache()

    val cost = model.computeCost(examples)    //每条数据与最近聚类中心的方差

    println("总代价(每条数据与最近聚类中心的方差)： $cost.")

    val centers = model.clusterCenters

    println("聚类中心：")
    centers.foreach(point => {
      println(point)
    })

    println("\n聚类详情：")
    val result = examples.map(line => {
      val cluster = model.predict(line)
      (cluster, line)
    }).sortByKey()
    result.foreach(x => println(x._1 + "| " + x._2.toArray.mkString("\t")))
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KMeans").setMaster("local")
    val sc = new SparkContext(conf)

    val inputPath = "ckooc-nlp/data/clustering/kmeans_standard_data.txt"
    val k = 15
    val numIterations = 100
    val input = sc.textFile(inputPath)
    val model = KMeans.train(input, k, numIterations)
    KMeans.print(input, model)
  }
}

// scalastyle:on println
