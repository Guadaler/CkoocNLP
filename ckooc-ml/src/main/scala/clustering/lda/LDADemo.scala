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

package clustering.lda

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 一个基于 ML pipeline 的LDA实现。
 * 运行命令：
 * {{{
 * bin/run-example ml.LDAExample
 * }}}
 */
object LDADemo {

  final val FEATURES_COL = "features"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val input = "G:/data/baike/test.txt".split(",")
    // 创建一个SparkContext和一个SQLContext
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val (corpus, vocab, tokensCount) = LDAUtils.preprocess(sc, input, 10000, "ckooc-ml/data/stopword.txt")

    // 加载数据
    /*val rowRDD = sc.textFile(input).filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))*/
    val rowRDD = corpus.map(_._2).map(_.toDense).map(Row(_))
    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT, nullable = false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)

    // 训练一个LDA模型
    val lda = new LDA()
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)
    val model = lda.fit(dataset)
    val transformed = model.transform(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    // 描述主题
    val topicsDF = model.describeTopics(10)

    val topicsRDD = topicsDF.map(x => (x.getInt(0), x.getSeq[Int](1), x.getSeq[Double](2)))

    val topics = topicsRDD.map { case (topic, terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocab(term.toInt), weight) }
    }

    println("10 topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }

    // 展示结果
//    topicsDF.show(false)
//    transformed.show(false)

    // $example off$
    sc.stop()
  }
}
// scalastyle:on println
