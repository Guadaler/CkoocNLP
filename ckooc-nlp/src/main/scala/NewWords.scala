import com.hankcs.hanlp.HanLP
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * 新词发现程序，处理步骤如下：
  *   <p>1. 使用HanLP分词
  *   <p>2. 按“最长短语长度”扩展短语
  *   <p>3. 去掉中间含标点、英文、数字的短语
  *   <p>4. 去掉低频短语，生成短语词典
  *   <p>5. 计算互信息
  *   <p>6. 计算左右熵
  *   <p>7. 整合互信息和左右熵（join）
  *   <p>8. 去掉短语两边的标点
  *   <p>9. 结果与词典进行过滤
  *
  * <p>Created by yhao on 2016/3/8.
  */
object NewWords {

  //主运行函数
  def run(sc: SparkContext, wordLength : Int, frequencyThreshold : Int, consolidateThreshold : Double, freedomThreshold : Double, text: String): RDD[(String, ((Int, Double), Double))] = {
    //英文字符正则
    val enExpr = "[A-Za-z]+"
    //数值正则，可以匹配203,2.23,2/12
    val numExpr = "\\d+(\\.\\d+)?(\\/\\d+)?"
    //匹配英文字母、数字、中文汉字之外的字符
    val baseExpr = """[^\w-\s+\u4e00-\u9fa5]"""

    val sourceRDD = genRDD(sc, text).map(line => line._2)
    val textLength = sourceRDD.map(line => line.length).reduce((a, b) => a + b)       // 计算总文本长度

    var wordsRDD = sourceRDD.map(line => {
      val tokensBuffer = new ArrayBuffer[String]()
      val tokens = HanLP.segment(line)
      val iter = tokens.iterator()
      while (iter.hasNext) {
        tokensBuffer.append(iter.next().word)
      }

      tokensBuffer
    }).filter(words => words.nonEmpty)
      .flatMap(words => generatephrase(words, wordLength + 1))

    //去掉含标点在中间or含英文or含阿拉伯数字的短语
    wordsRDD = wordsRDD.filter{case(phrase, words) =>
      var flag = true
      words.foreach(word => { if (word.matches(numExpr) || word.matches(enExpr)) flag = false})
      if (words.size > 2) {
        //过滤中间的词串中有标点等符号的短语
        val subWords = words.tail.take(words.size - 2)
        subWords.foreach(word => { if (word.matches(baseExpr) || word.equals(" ")) flag = false})
      }
      flag
    }

    // 开始分词
    // Step1 : 词频  Step2 ； 凝结度   Step3 : 自由熵
    // 抽词，生成词长1-6的词频词典
    val FreqDic = wordsRDD.map(phrases => ((phrases._1, phrases._2), 1)).reduceByKey(_ + _)

    //  对待查词表的初步过滤，留下长度为1-5，词频>wordFilter且不在常用词字典中的词
    val candidateWords = FreqDic.filter(_._2 > frequencyThreshold)
    candidateWords.persist()    // 得到candidate及词频

    val disDictionary = FreqDic.map(phrase => (phrase._1._1, phrase._2)).collect()    // 一个 Array[((String, scala.Seq[String]), Int)]
    Sorting.quickSort(disDictionary)(Ordering.by[(String, Int), String](_._1))    //对字典进行排序
    val broadforwardDic = sc.broadcast(disDictionary)     // 广播正序字典

    // 过滤长度为1个词的短语
    // Step2 计算凝结度 返回：( String , (Int ,Double))(词，（频率，凝结度）)
    val consolidateRDD = candidateWords.filter(line => line._1._2.size > 1).map(line => countDoc(line, textLength, broadforwardDic.value)).reduceByKey{(value1, value2) => {
      val freq = value1._1 + value2._1
      val consolidate = value1._2 * value1._1 / freq + value2._2 * value2._1 / freq
      (freq, consolidate)
    }}

    // Step3 计算自由熵
    //右自由熵
    val rightFreedomRDD = candidateWords.map{case (key ,value) => countDof((key._1, value), broadforwardDic.value)}
    broadforwardDic.unpersist()

    val wordsTimesReverse = FreqDic.map(phrase => (phrase._1._1, phrase._2)).map{case (key ,value) => ( key.reverse , value)}   // 逆序词典
    val disDictionaryReverse = wordsTimesReverse.collect()
    Sorting.quickSort(disDictionaryReverse)(Ordering.by[(String, Int), String](_._1))
    val broadbackwardDic = sc.broadcast(disDictionaryReverse)                                       // 广播逆序字典

    // 左自由熵
    val leftFreedomRDD = candidateWords.map{case (key ,value) => countDof ((key._1.reverse , value) ,broadbackwardDic.value)}.map{case (key,value) => (key.reverse, value)}
    broadbackwardDic.unpersist()

    // 整合三个特征
    val freedomRDD_temp = rightFreedomRDD.join(leftFreedomRDD)
    val freedomRDD = freedomRDD_temp.map({case (key, (value1, value2)) => (key, math.min(value1, value2))})
    var resultRDD = consolidateRDD.join(freedomRDD)

    //去掉短语两边的符号
    resultRDD = resultRDD.map{case((phrase, ((freq, consolidate), freedom))) => (phrase.replaceAll(baseExpr, ""), ((freq, consolidate), freedom))}.filter(_._1.nonEmpty)

    resultRDD = resultRDD.filter(_._1.length > 1)

    // 经过阈值过滤得到最终的词
    resultRDD.filter{case((phrase, ((freq, consolidate), freedom))) => calculation(freq, consolidate,freedom ,frequencyThreshold ,consolidateThreshold ,freedomThreshold)}.distinct()
  }


  //读取数据转化为RDD
  def genRDD(sc: SparkContext, inPath: String, sep: Char = 239.toChar) = {
    val hadoopConf = sc.hadoopConfiguration
    val fs = new Path(inPath).getFileSystem(hadoopConf)
    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024)
    val minPart = (len / 16).toInt

    var rdd: RDD[String] = null
    val newpath = inPath

    if (minPart > 10) {
      rdd = sc.textFile(newpath, minPart)
    } else {
      rdd = sc.textFile(newpath)
    }

    var resultRdd = rdd.mapPartitions(str => str.map(_.split(sep)).filter(_.length == 14)).mapPartitions(str => str.map { cols =>
    {
      (cols(0).toLong, cols(6) + cols(13))
    }
    })
    if (minPart > 10) {
      //减小空运行的任务数量，提高task的利用率
      resultRdd = resultRdd.coalesce(minPart)
    }
    resultRdd
  }

  //文本中出现明文的\r\n等转以符号
  def dealWithSpecialCase(content: String): ArrayBuffer[String] = {
    val patterns = Array("\\\\r\\\\n", "\\\\n", "\\\\t", "[0-9]{8,}")
    val tmp = mutable.ArrayBuffer[String]()
    val ret = mutable.ArrayBuffer[String]()
    ret += content
    for (pat <- patterns) {
      tmp.clear()
      for (ele <- ret) {
        val e = ele.trim()
        if (e != "") {
          tmp ++= e.replaceAll(pat, "|").split( """\|""")
        }
      }
      ret.clear()
      ret ++= tmp.clone()
    }
    ret
  }

  //判断符号是否有意义
  def isMeaningful(ch: Char): Boolean = {
    var ret = false
    val meaningfulMarks = Array('*', '-', 'X', '.','\\')
    if ((ch >= '一' && ch <= '龥') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || meaningfulMarks.contains(ch))
      ret = true
    ret
  }

  //把每一行分成长度1-wordLength+1长度的词，时间复杂度为n^2
  def generatephrase(v: Seq[String], wordLength: Int ):ArrayBuffer[(String, Seq[String])] = {
    val words = v.toArray
    val greetStrings =  mutable.ArrayBuffer[(String, Seq[String])]()
    for (i <- 0 to words.length -2) {
      var j = i + 1
      if (words(i).length < wordLength) {
        greetStrings.append((words(i), Seq(words(i))))

        var headLenth = words(i).length
        while (j < words.length && headLenth + words(j).length < wordLength) {
          var temp = new ArrayBuffer[String]()
          for (k <- i to j) {
            temp += words(k)
          }
          greetStrings.append((temp.mkString(""), temp))
          headLenth += words(j).length
          j += 1
        }
      }
    }

    if (words.last.length < wordLength) {
      greetStrings.append((words.last, Seq(words.last)))
    }
    greetStrings
  }

  //过滤得到candidate word：长度1-5，频率大于wordfilter
  def filterFunc( line : (String , Int) , wordLength : Int , wordFilter : Int) : Boolean= {
    val len = line._1.length
    val frequency = line._2
    len >= 1 && len <= wordLength && frequency > wordFilter
  }
  //过滤掉stopword
  def filterStopWords ( words : String , dictionary : Array[ String]) : Boolean = {
    if (dictionary.contains( words ))
      false
    else
      true
  }
  //从广播变量中，用二分查找，查找到某个词的位置和频率
  def BinarySearch(word: String , dictionary : Array[(String ,Int)], from: Int = 0, to: Int) : (Int, Int) = {
    var L = from
    var R = to - 1
    var mid : Int = 0
    while (L < R) {
      mid = (L + R) / 2
      if (word > dictionary(mid)._1)
        L = mid + 1
      else
        R = mid
    }
    if (word != dictionary(L)._1){
      println("NOT FIND WORD" + word)
      return (-1, 0)
    }
    (L,  dictionary(L)._2)
  }

  //计算凝结度
  def countDoc(phrase: ((String, scala.Seq[String]), Int), TextLen: Int, dictionary : Array[(String ,Int)]): (String , (Int ,Double)) = {
    var tmp = TextLen.toDouble
    val len = dictionary.length
    val words = phrase._1._2.toArray
    if (words.length > 1) {
      for (num <- 0 to words.length - 2) {
        val Lword = words.take(num + 1).mkString("")
        val Rword = words.takeRight(words.length - num - 1).mkString("")
        val searchDicLword = BinarySearch(Lword, dictionary, 0, len)
        val searchDicRword = BinarySearch(Rword, dictionary, 0, len)
        if (searchDicLword._1 == -1 || searchDicRword._1 == -1) {
          println("Lword: " + Lword)
          println("Rword: " + Rword)
          println("No words found")
        }
        else {
//          tmp = math.min(tmp, phrase._2.toDouble * TextLen / searchDicLword._2.toDouble / searchDicRword._2.toDouble)
          tmp = math.min(tmp, math.log(phrase._2.toDouble * TextLen / searchDicLword._2.toDouble / searchDicRword._2.toDouble))
        }
      }
    }
    if (tmp < 0)
      println(phrase._1._1 + ": " + phrase._2)
    (phrase._1._1, (phrase._2, tmp))
  }

  //计算自由熵
  def countDof(wordLine: (String , Int) , dictionary : Array[(String ,Int)]) : (String , Double) = {
    val word = wordLine._1
    val Len = word.length
    var total = 0
    var dof = mutable.ArrayBuffer[Int]()
    var pos = BinarySearch(word, dictionary, 0, dictionary.length)._1
    val dictionaryLen = dictionary.length
    /*if ( pos == 0){
      print(word)
      println( "No words found")
      return ( word , 0)
    }*/
    while( pos < dictionaryLen && dictionary(pos)._1.startsWith(word)) {
      val tmp = dictionary(pos)._1
      if (tmp.length > Len) {
        val freq = dictionary(pos)._2
        dof += freq
        total += freq
      }
      pos += 1
    }
    ( word ,dof.map((x:Int) => 1.0*x/total).map((i:Double) => -1 * math.log(i)*i).foldLeft(0.0)((x0, x)=> x0 + x))
  }

  //设定阈值过滤结果
  def calculation ( f : Int , c : Double , fr : Double , frequency : Int , consolidate : Double , free : Double) : Boolean = {
    f >= frequency && c >= consolidate && fr >= free
  }


  //从广播变量中，用二分查找，查找到某个词的位置和频率
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
      println("NOT FIND WORD" + word)
      return -1
    }
    L
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StatisticsNewWords").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 获取参数

    val args = Array("5", "2", "0", "0", "data/sample_data1.txt", "data/sample_data2.txt", "library/dictionary-all.dic")

    val wordLength : Int = args(0).toInt   // 最长短语长度
    val frequencyThreshold : Int = args(1).toInt   // 最小词频阈值
    val consolidateThreshold : Double = args(2).toDouble  // 最小凝结度阈值
    val freedomThreshold : Double = args(3).toDouble  // 最小自由熵阈值

    val text1 = args(4)
    val text2 = args(5)

    val dictionary = sc.textFile(args(6)).collect()
    val broadDic = sc.broadcast(dictionary)

    val resultRDD1 = run(sc, wordLength, frequencyThreshold, consolidateThreshold, freedomThreshold, text1)
    val resultRDD2 = run(sc, wordLength, frequencyThreshold, consolidateThreshold, freedomThreshold, text2)

    val result = resultRDD1.map(phrase => phrase._1).collect().toSet
    val broadResult = sc.broadcast(result)

    var tempRDD = resultRDD2.filter(phrase => !broadResult.value.contains(phrase._1))

    tempRDD = tempRDD.map(word => {
      var term = ""
      val dic = broadDic.value
      if (binarySearch(word._1, dic, 0, dic.length - 1) == -1) {
        term = word._1
      }
      (term, word._2)
    }).filter(_._1.nonEmpty).distinct()

    val finalRDD = tempRDD.map(line => {
      val phrase = line._1
      val freq = line._2._1._1
      phrase + " " + freq + " " + line._2._1._2 + " " + (line._2._2 * 10000).floor / 10000
    })

//    finalRDD.saveAsTextFile(args(7))
    finalRDD.foreach(println)

    sc.stop()
  }
}
