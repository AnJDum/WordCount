# 大数据作业五

#### 191098273 徐冰清

## 作业要求

在HDFS上加载莎士比亚文集的数据文件（shakespeare-txt.zip解压后目录下的所有文件），编写MapReduce程序进行词频统计，并按照单词出现次数从大到小排列，输出（1）每个作品的前100个高频单词；（2）所有作品的前100个高频单词，要求忽略大小写，忽略标点符号（punctuation.txt），忽略停词（stop-word-list.txt），忽略数字，单词长度>=3。输出格式为"<排名>：<单词>，<次数>“，输出可以根据作品名称不同分别写入不同的文件。

## 环境配置

电脑：big sur 11.6

hadoop：3.3.1

java：openjdk 16

IntelliJ IDEA：2021.2.3（Community）



## 运行截图

通过IntelliJ IDEA生成jar包，将input与停词文件传入docker，再传入dfs。在命令行输入`./hadoop jar wordcount.jar WordCount /1/input /1/output -skip /1/punctuation.txt /1/stop-word-list.txt`，运行jar包，运行截图如下：

<img src="/img/截屏2021-10-30 下午8.30.36.png" alt="截屏2021-10-30 下午8.30.36" style="zoom:50%;" />

<img src="/img/截屏2021-10-30 下午8.29.25.png" alt="截屏2021-10-30 下午8.29.25" style="zoom:50%;" />

运行结果如下：

<img src="/img/截屏2021-10-30 下午10.31.14.png" alt="截屏2021-10-30 下午10.31.14" style="zoom:50%;" />

<img src="/img/截屏2021-10-30 下午10.30.48.png" alt="截屏2021-10-30 下午10.30.48" style="zoom:50%;" />



web截图如下：

<img src="/img/截屏2021-10-30 下午8.35.47.png" alt="截屏2021-10-30 下午8.35.47" style="zoom:50%;" />


<img src="/img/截屏2021-10-30 下午8.37.41.png" alt="截屏2021-10-30 下午8.37.41" style="zoom:50%;" />

<img src="/img/截屏2021-10-30 下午8.38.00.png" alt="截屏2021-10-30 下午8.38.00" style="zoom:50%;" />

<img src="/img/截屏2021-10-30 下午8.38.08.png" alt="截屏2021-10-30 下午8.38.08" style="zoom:50%;" />

<img src="/img/截屏2021-10-30 下午8.38.24.png" alt="截屏2021-10-30 下午8.38.24" style="zoom:50%;" />

## 设计思路

总体思路：分四个job，分别循序渐进地串行处理四个工作。

#### **job1**

作用：排除了不需要的单词，统计所有文件单词出现的次数，输出到临时文件。

整体架构与原有的WordCount2.0相似，在parseSkipFile加入对停词文件的处理、在map中限制字数不小于3；加入FileOutputFormat输出中间文件。



#### **job2**

作用：读取job1输出的中间文件，对文件内容进行排序，并按照指定格式输出。

在job1成功运行的基础上，读取文件内容。编写`RankOutputFormat`，对单词进行排序，同时调整输出格式。其中需要排序时hadoop默认的升序比较类改为自定义的降序比较类。



#### **job3**

作用：用于读取所有文件的单词，在记录单词出现次数时，分文件地将单词出现次数记录下来。

创建`FileTokenizerMapper`，写文件时加入文件名，便于在job4中区分单词归属文件。



#### **job4**

作用：读取job3输出的中间文件，按照fileList对文件内容进行排序，并按照指定格式输出。

通过`InverseMapper`交换单词和数字的位置，对单词出现的次数进行排序。编写`FileSortReducer`，将文件按照文件名存入file-sort文件夹。

完整的过程可以用如下的流程图表示：
   <img src="/img/截屏2021-10-30 下午10.53.56.png" alt="截屏2021-10-30 下午10.53.56" style="zoom:50%;" />


## 存在的不足和可以改进之处

1. 程序执行效率较低，在我的电脑上花了大约十四分钟才跑完全部数据，数据读取传输多次，消耗内存，增加通信负担。
2. 各类重复性大，文件中复制粘贴部分多，冗长。可以修改map和reduce接口，使得相同的功能不再重复，增强代码的可阅读性。
 

## 遇到的错误

#### **配置阶段**

先尝试运行wordcount2.0，使用IntelliJ IDEA的maven进行配置。

1. **Hadoop包不存在的问题**

   <img src="/img/截屏2021-10-30 下午11.04.03.png" alt="截屏2021-10-30 下午11.04.03" style="zoom:50%;" />

   <img src="/img/截屏2021-10-30 下午11.04.12.png" alt="截屏2021-10-30 下午11.04.12" style="zoom:50%;" />

   点击M字母，输入`idea:idea`，回车键运行，由此解决Hadoop包不存在的问题。

2. **Cannot resolve symbol 'conf'等诸多bug**

   检查clean 和 install，clean可以正常运行，install不可以。出现报错`Maven [ERROR] 不再支持源选项 5，请使用 7 或更高版本的解决办法`.在porn.xml增加一段配置，指定jdk版本，解决。

   <img src="/img/截屏2021-10-30 下午11.04.47.png" alt="截屏2021-10-30 下午11.04.47" style="zoom:50%;" />

   

3. **错误: 找不到或无法加载主类 WordCount2**

   class建到test里面去了。重新在main文件夹里面建class，成功运行。

```java
job.addCacheFile(new Path(remainingArgs[2]).toUri());
job.addCacheFile(new Path(remainingArgs[3]).toUri());
job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
```

4. **缺少-skip**

在idea里面没有-skip，因此在`main`中把把停词文件单独写出来，设置"wordcount.skip.patterns"为true，便于单机调试。

```java
List<String> otherArgs = new ArrayList<String>();
job.addCacheFile(new Path(remainingArgs[2]).toUri());
job.addCacheFile(new Path(remainingArgs[3]).toUri());       
job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
```

