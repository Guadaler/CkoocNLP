***********************************************************************
# ckoocnlp
ickooc自然语言处理


***********************************************************************

# 数据预处理
NLP数据预处理代码实现
====================================
[数据预处理代码](https://github.com/yhao2014/CkoocNLP/blob/master/ckooc-ml/src/main/scala/algorithm/utils/PreProcessUtils.scala)
，主要进行以下操作：<br>
>
* 繁简转换
* 全半角转换
* 去除无意义词
* 分词
* 去除停用词
* 去除低频词

输入数据格式
----------
数据预处理的输入数据为[知乎]()上抓取的数据，格式如下：<br>
`问题标识`,`一级分类`,`所属话题`,`标签词`,`作者标识`,`作者名称`,`标题`,`最后更新时间`,`关注人数`,`浏览次数`,`相关话题关注人数`,`评论数`,`回复数`,`内容`<br>
切分符：`\u00EF`<br>
输入文件位置：data/sample_data2.txt

输出数据格式
----------
数据预处理的输出数据格式如下：<br>
`问题标识`,`分词后的标题+内容`<br>
切分符：`\u00EF`<br>
输出文件位置：data/preprocess_result.txt


# spark-LDA
基于spark的LDA的scala实现
====================================

这是一个基于[spark](http://spark.apache.org/)的常规定义的
[LDA](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)的Scala代码实现.
本代码根据spark官网提供的
[LDAExample.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/LDAExample.scala)
文件进行代码改进，实现了LDA的模型训练和保存、加载模型并进行预测、基于LDA预测结果的相似文档查找功能。
下面是一些具体的说明。

LDA模型训练
-----------
[LDA模型训练](https://github.com/yhao2014/CkoocNLP/blob/master/ckooc-ml/src/main/scala/algorithm/clustering/lda/LDATrainDemo.scala)
代码实现，主要对数据预处理后（分词等）的数据进行LDA模型训练，并对训练的LDAModel和对应的切分tokens进行保存。<br>
输入数据为经过分词预处理的数据，格式如下：<br>
`问题标识`,`分词后的标题+内容`<br>
切分符：`|`<br>
输入文件位置：data/preprocess_result.txt

LDA新文档预测
------------
[LDA新文档预测](https://github.com/yhao2014/CkoocNLP/blob/master/ckooc-ml/src/main/scala/algorithm/clustering/lda/LDAPredictDemo.scala)
代码实现，主要对数据预处理后（分词等）的数据，加载训练好的LDAMOdel后，进行训练，并得到其`文档-主题分布`和`主题-词`结果。<br>
输入数据为经过分词预处理的数据，格式如下：<br>
`问题标识`,`分词后的标题+内容`<br>
切分符：`|`<br>
输入文件位置：data/preprocess_result.txt

**模型下载**已经在Windows下训练出一个可以用于测试的LDA模型，因为比较大，需要单独下载，解压放入ckooc-ml下models目录中即可
模型说明：
>
* docNum=50000（知乎问题数据）
* K=200
* maxIterations=40
* vocabSize=10000
* algorithm=em

模型下载地址：[http://pan.baidu.com/s/1slcLdr3](http://pan.baidu.com/s/1slcLdr3) 密码：ir0e

相似文档查找
----------
[相似文档查找](https://github.com/yhao2014/CkoocNLP/blob/master/ckooc-ml/src/main/scala/application/LDASimiDocDemo.scala)
代码实现，主要结合了LDA的新文档预测，将预测得到的`文档-主题分布`进行`余弦距离计算`，并进行排序，得到每篇文档对应的`主题相似度最高`的文档。<br>
输入数据为两份经过分词预处理的数据，格式如下：<br>
`问题标识`,`分词后的标题+内容`<br>
切分符：`|`<br>
输入文件位置：data/sample_split_data1.txt、data/sample_split_data2.txt

**上述所有代码均可直接执行**

LDA模型训练性能记录
----------------
        数据           CPU           内存           数据           语料大小           文本特点           文本个数           迭代次数          主题数          训练时长          avglogLikelihood          logPerplexity
    百度百科       5cores * 16     50g * 16         99M            4721710             长文本             10000                10               500            4.28min            -10374.58748             -20994.3680
    百度百科       5cores * 16     50g * 16         99M            4721814             长文本             10000                20               500            7.47min            -8786.095098             -24717.5388
    百度百科       5cores * 16     50g * 16         99M            4721490             长文本             10000                30               500              11min            -8517.495272             -24321.77698
    百度百科       5cores * 16     50g * 16         99M            4721450             长文本             10000                40               500           12.27min            -8451.208588             -25191.88867
    百度百科       5cores * 16     50g * 16         99M            4721573             长文本             10000                50               500           18.42min            -8424.327683             -24954.14616
    百度百科       5cores * 16     50g * 16         99M            4721609             长文本             10000                60               500           20.35min            -8410.599461             -24732.90191
    百度百科       5cores * 16     50g * 16         99M            4721452             长文本             10000                70               500            24.5min            -8404.112362             -25228.67469
    百度百科       5cores * 16     50g * 16         99M            4721814             长文本             10000                80               500           27.76min            -8401.891865             -24780.33578

    5cores * 12   30g * 12       百度百科          428M             长文本            100000              40              1000            2.82h


# 新词发现
基于左右熵、互信息以及固有词典的新词发现代码实现
========================================

这是基于[左右熵和互信息](http://www.hankcs.com/nlp/extraction-and-identification-of-mutual-information-about-the-phrase-based-on-information-entropy.html)
以及固有词典过滤的新词发现代码实现。主要处理逻辑如下：<br>
>
* 使用HanLP分词
* 按“最长短语长度”扩展短语
* 去掉中间含标点、英文、数字的短语
* 去掉低频短语，生成短语词典
* 计算互信息
* 计算左右熵
* 整合互信息和左右熵（join）
* 去掉短语两边的标点
* 结果与词典进行过滤

输入数据格式
----------
输入数据为两份经过分词预处理的数据，格式如下：<br>
`问题标识`,`分词后的标题+内容`<br>
切分符：`|`<br>
输入文件位置：data/sample_split_data1.txt、data/sample_split_data2.txt

**新词发现代码还未进行优化！**
