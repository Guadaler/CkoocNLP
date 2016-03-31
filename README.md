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
切分符：`/u00EF`<br>
输入文件位置：ckooc-ml/data/preprocess_sample_data.txt

输出数据格式
----------
数据预处理的输出数据格式如下：<br>
`问题标识`,`分词后的标题+内容`<br>
切分符：`/u00EF`<br>
输出文件位置：ckooc-ml/data/preprocess_result.txt


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
代码实现，主要对数据预处理后（分词等）的数据进行LDA模型训练，并对训练的LDAModel和对应的切分tokens进行保存。

LDA新文档预测
------------
[LDA新文档预测](https://github.com/yhao2014/CkoocNLP/blob/master/ckooc-ml/src/main/scala/algorithm/clustering/lda/LDAPredictDemo.scala)
代码实现，主要对数据预处理后（分词等）的数据，加载训练好的LDAMOdel后，进行训练，并得到其`文档-主题分布`和`主题-词`结果。

相似文档查找
----------
[相似文档查找](https://github.com/yhao2014/CkoocNLP/blob/master/ckooc-ml/src/main/scala/application/LDASimiDocDemo.scala)
代码实现，主要结合了LDA的新文档预测，将预测得到的`文档-主题分布`进行`余弦距离计算`，并进行排序，得到每篇文档对应的`主题相似度最高`的文档。


**上述所有代码均可直接执行**

LDA模型训练性能记录
----------------
        CPU          内存          数据          语料大小          文本特点          文本个数          迭代次数          主题数          训练时长
    10cores * 8   150g * 8       知乎问题          133M             长文本            204103              40              1000             1.1h
