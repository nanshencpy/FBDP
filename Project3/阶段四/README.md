阶段四任务（数据挖掘）：

* 使用Spark MLlib中Logistic、SVM、NaiveBayes和RandomForest编写程序；

* 将train_after按照70%:30%划分成训练集和测试集；

* 使用accuracy_score对预测的准确率进行评估；

* 通过改变训练集中正反例的比例，每个算法训练十个模型，绘出训练集中正反例比例与预测的准确率的图像；

* data.txt是对test_after的预测。

```
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, SVMWithSGD, NaiveBayes
from pyspark.mllib.tree import RandomForest
from sklearn.metrics import accuracy_score
sc = SparkContext(appName="classification", master='local')

test = sc.textFile('test.csv')
train = sc.textFile('train.csv')
calculate = sc.textFile('test_after.csv')

test_key = test.map(lambda x: list(map(int, x.split(',')[0: 4])))
test_value = test.map(lambda x: list(map(int, x.split(',')[-1])))
train_data = train.map(lambda x: (int(x.split(',')[-1]), list(map(int, x.split(',')[0: 4]))))
calculate_data = calculate.map(lambda x: (int(x.split(',')[-1]), list(map(int, x.split(',')[0: 4]))))

l_p = []
s_p = []
n_p = []
r_p = []
best_item = 0
best_f1 = 0

for item in range(1, 10, 1):
    train_data_0 = train_data.filter(lambda x: x[0] == 0).sample(False, item / 10.0, 0)
    train_data_1 = train_data.filter(lambda x: x[0] == 1).sample(False, 0.5, 0)
    train_data_final = train_data_0.map(lambda x: LabeledPoint(x[0], x[1])).union(train_data_1.map(lambda x: LabeledPoint(x[0], x[1])))
    lrm = LogisticRegressionWithLBFGS.train(train_data_final, iterations=10)
    l_p.append(accuracy_score(test_value.collect(), lrm.predict(test_key).collect()))
    svm = SVMWithSGD.train(train_data_final, iterations=10)
    s_p.append(accuracy_score(test_value.collect(), svm.predict(test_key).collect()))
    nb = NaiveBayes.train(train_data_final)
    n_p.append(accuracy_score(test_value.collect(), nb.predict(test_key).collect()))
    rf = RandomForest.trainClassifier(train_data_final, 2, {}, 10)
    r_p.append(accuracy_score(test_value.collect(), rf.predict(test_key).collect()))

import matplotlib.pyplot as plt 
x_axis  = list(map(lambda x: x * train_data_0.count() / train_data_1.count() / 10 / 0.5, list(range(1, 10, 1))))

plt.plot(x_axis, l_p, label='Logistic')
plt.plot(x_axis, s_p, label='SVM')
plt.plot(x_axis, n_p, label='NaiveBayes')
plt.plot(x_axis, r_p, label='RandomForest')
plt.legend()
plt.savefig('acc.jpg', dpi = 900)

sc.stop()

calculate_data = calculate.map(lambda x: list(map(int, x.split(',')[0: 4])))
s = nb.predict(calculate_data ).collect()
with open('data.txt','w') as f:
    for item in s:
        f.write(str(item))
        f.write('\n')

```
