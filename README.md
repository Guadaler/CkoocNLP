***********************************************************************
# ckoocnlp
ickooc自然语言处理


***********************************************************************

# 数据预处理
NLP数据预处理代码实现
====================================
>>数据预处理代码，主要进行以下操作：<br>
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

数据格式
-----------

The input data format is similar to the [JGibbLDA input data
format](http://jgibblda.sourceforge.net/#_2.3._Input_Data_Format), with some
minor cosmetic changes and additional support for document labels necessary for
Labeled LDA. We first describe the (modified) input format for unlabeled
documents, followed by the (new) input format for labeled documents.

**Changed from JGibbLDA**: All input/output files must be Gzipped.

### Unlabeled Documents

Unlabeled documents have the following format:

    document_1
    document_2
    ...
    document_m

where each document is a space-separated list of terms, i.e.,:

    document_i = term_1 term_2 ... term_n

**Changed from JGibbLDA**: The first line *should not* be an integer indicating
the number of documents in the file. The original JGibbLDA code has been
modified to identify the number of documents automatically.

**Note**: Labeled and unlabeled documents may be mixed in the input file, thus
you must ensure that unlabeled documents do not begin with a left square bracket
(see Labeled Document input format below). One easy fix is to prepend a space
character (' ') to each unlabeled document line.

### Labeled Documents

Labeled documents follow a format similar to unlabeled documents, but the with
labels given at the beginning of each line and surrounded by square brackets,
e.g.:

    [label_1,1 label_1,2 ... label_1,l_1] document_1
    [label_2,1 label_2,2 ... label_2,l_2] document_2
    ...
    [label_m,1 label_m,2 ... label_m,l_m] document_m

where each label is an integer in the range [0, K-1], for K equal to the number
of topics (-ntopics).

**Note**: Labeled and unlabeled documents may be mixed in the input file. An
unlabeled document is equivalent to labeling a document with every label in the
range [0, K-1].

Usage
-----

Please see the [JGibbLDA usage](http://jgibblda.sourceforge.net/#_2.2._Command_Line_&_Input_Parameter), noting the following changes:

*   All input files must be Gzipped. All output files are also Gzipped.

*   New options have been added:

    **-nburnin <int>**: Discard this many initial iterations when taking samples.

    **-samplinglag <int>**: The number of iterations between samples.

    **-infseparately**: Inference is done separately for each document, as if
    inference for each document was performed in isolation.

    **-unlabeled**: Ignore document labels, i.e., treat every document as
    unlabeled.

*   Some options have been deleted:

    **-wordmap**: Filename is automatically built based on model path.

Example:
    //training
    java -mx400G -cp bin:lib/args4j-2.0.6.jar -jar JGibbLabeledLDA.jar -est -model JLDAModel -alpha 0.5 -beta 0.1 -ntopics 100 -niters 100 -twords 20 -dir /home/spark/yhao/model -dfile traindata.gz

    //inferring
    java -mx512M -cp bin:lib/args4j-2.0.6.jar -jar JGibbLabeledLDA.jar -inf -dir /home/spark/yhao/model -model JLDAModel -niters 30 -twords 20 -dfile testdata.gz
Contact
-------

Please direct questions to [Myle Ott](myleott@gmail.com).

License
-------

Following JGibbLDA, this code is licensed under the GPLv2. Please see the
LICENSE file for the full license.

Labeled LDA in Java
Copyright (C) 2008-2013 Myle Ott (Labeled LDA), Xuan-Hieu Phan and Cam-Tu Nguyen (JGibbLDA)

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

