# Homework_4

### Background
  大数据处理课程作业四
### Usage
* 矩阵乘法程序 MatrixMultiply.java
```
命令行参数：<M矩阵的文件路径> <N矩阵的文件路径> <输出文件路径>
```
* 关系代数程序 RelationAlgebra.java
```
命令行参数：<输入文件的上级文件夹路径>
```
* 倒排索引程序 InvertedIndex.java
```
命令行参数：<输入文件的上级文件夹路径> <输出文件路径>
```
### Advantages
* MatrixMultiply
   1. reduce函数无需知道数据来自哪个文件，避免了将数据来源在 Mapper 和 Reducer 之间进行传递，提高了运行效率；
   2. 将坐标自定义为数据结构，提高了代码的可读性，降低了开发和更新的难度；
   3. 将坐标第二位的数据类型设置为DoubleWritable，考虑了矩阵值数据类型的多样性，增强了程序的鲁棒性；
* RelationAlgebra
   1. 多个mapreduce程序叠加使用，实现一次运行得到所有结果；
* InvertedIndex
   1. 重载Text的compareTo(BinaryComparable other)函数，按照单词和词频进行有序输出；
   2. 使用combiner，提高了运行效率；
### Developer
  南京大学陈鹏宇
