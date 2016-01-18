package spark.mllib.basicstatistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.stat.test.{BinarySample, StreamingTest, StreamingTestResult}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yhao on 2016/1/16.
  */
object StreamingSignificanceTestingDemo extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
  private val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))

  private val dataDir: String = args(0)
  val data = ssc.textFileStream(dataDir).map(line => line.split(",") match {
    case Array(label, value) => BinarySample(label.toBoolean, value.toDouble)
  })

  private val streamingTest: StreamingTest = new StreamingTest()
    .setPeacePeriod(0)
    .setWindowSize(0)
    .setTestMethod("welch")

  private val out = streamingTest.registerStream(data)
  out.print()
}
