# Homework_4

### Background
  大数据处理课程作业四
### Install & Usage
* 矩阵乘法程序
```
bin/hadoop jar MatrixMultiply.jar <M矩阵的文件路径> <N矩阵的文件路径> <输出文件路径>
```
  另附源代码：MatrixMultiply.java
* 关系代数程序
```
bin/hadoop jar RelationAlgebra.jar <集合的上级文件夹路径>
```
  另附源代码：RelationAlgebra.java
### Advantages
* MatrixMultiply
   1. reduce函数无需知道数据来自哪个文件，避免了将数据来源在 Mapper 和 Reducer 之间进行传递，提高了运行效率；
   2. 将坐标自定义为数据结构，提高了代码的可读性，降低了开发和更新的难度；
   3. 将坐标第二位的数据类型设置为DoubleWritable，考虑了矩阵值数据类型的多样性，增强了程序的鲁棒性；
   4. 待写
* RelationAlgebra
   1. 待写
### Developer
  南京大学陈鹏宇
