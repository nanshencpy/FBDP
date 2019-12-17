阶段四任务（数据挖掘）：

* 使用Spark MLlib中Logistic、SVM、NaiveBayes和RandomForest编写程序；

* 使用F1对预测的准确率进行评估；

* 通过改变训练集中正反例的比例，每个算法训练一百个模型，绘出训练集中正反例比例与预测的准确率的图像；
```
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, SVMWithSGD, NaiveBayes
from pyspark.mllib.tree import RandomForest
from sklearn.metrics import f1_score
import matplotlib.pyplot as plt 
sc = SparkContext(appName="classification", master='local')
test_data = sc.textFile('test_after.csv')
train_data = sc.textFile('train_after.csv')
test_key = test_data.map(lambda x: list(map(int, x.split(',')[0: 4])))
test_value = test_data.map(lambda x: list(map(int, x.split(',')[-1])))
train_data = train_data.map(lambda x: (int(x.split(',')[-1]), list(map(int, x.split(',')[0: 4]))))
l_f1 = []
s_f1 = []
n_f1 = []
r_f1 = []
for item in range(1, 100, 1):
    train_data_0 = train_data.filter(lambda x: x[0] == 0).sample(False, item / 1000.0, 0)
    train_data_1 = train_data.filter(lambda x: x[0] == 1)
    train_data_final = train_data_0.map(lambda x: LabeledPoint(x[0], x[1])).union(train_data_1.map(lambda x: LabeledPoint(x[0], x[1])))
    lrm = LogisticRegressionWithLBFGS.train(train_data_final , iterations=10)
    l_f1.append(f1_score(test_value.collect(), lrm.predict(test_key).collect()))
    svm = SVMWithSGD.train(train_data_final, iterations=10)
    s_f1.append(f1_score(test_value.collect(), svm.predict(test_key).collect()))
    nb = NaiveBayes.train(train_data_final)
    n_f1.append(f1_score(test_value.collect(), nb.predict(test_key).collect()))
    rf = RandomForest.trainClassifier(train_data_final, 2, {}, 10)
    r_f1.append(f1_score(test_value.collect(), rf.predict(test_key).collect()))
x_axis = list(range(1, 100, 1))
x_axis = x_axis / 1000 * 9432 / 568
plt.plot(x_axis, l_f1, label='Logistic')
plt.plot(x_axis, s_f1, label='SVM')
plt.plot(x_axis, n_f1, label='NaiveBayes')
plt.plot(x_axis, r_f1, label='RandomForest')
plt.legend()
plt.savefig('f1.jpg', dpi = 900)
```
