import tensorflow as tf
#coding:utf-8
import re
import jieba
import jieba.posseg as pseg # 词性标注
#from jieba.analyse import ChineseAnalyzer
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from jieba.analyse import extract_tags

# 去除标点
# 去除停用词
# 分词
# 文本的数值化
# 文本分类模型
# 训练词向量，聚类，KNN，朴素贝叶斯
# 文档频率，主成分分析、互信息、信息增益、χ²统计量
# 输入层，卷积层，池化层，全连接层
# 评估，调优

import os
import re

with open('./doc/protitle.txt', "r") as f:
    word_all = f.read()

word_all=''.join(re.findall(u'[a-zA-Z\d\u4e00-\u9fff]+', word_all))

weight_arr=extract_tags(''.join(re.findall(u'[a-zA-Z\u4e00-\u9fff]+', word_all)), topK=3, withWeight=True, allowPOS=())
speech_arr=pseg.cut(word_all)
print(word_all)

from sklearn.feature_extraction.text import CountVectorizer  
vectorizer=CountVectorizer()
data=pd.read_excel(u'./resource/data/源数据.xlsx')

secondtype_mapping = {
       u'裙子':1,
       u'上衣':2,	
       u'日用':3,
       u'配饰':4,
       u'套装':5,
       u'鞋':6,
       u'裤子':7,
       u'包包':8,
       u'链表':9,
       u'家纺':10,
       u'外套':11,
       u'连衣裙':12,
       u'其它':13
}
data[u'二级类目'] = data[u'二级类目'].map(secondtype_mapping)
data.fillna(0)

# print(vectorizer.fit_transform(data[u'产品标题']))
print(vectorizer.fit_transform(data[u'产品标题']).toarray())
# print(vectorizer.get_feature_names())

from sklearn.feature_extraction.text import HashingVectorizer 
vectorizer2=HashingVectorizer(n_features = 100,norm = None)

data_Y=data[u'二级类目']
#data_X=vectorizer.fit_transform(data[u'产品标题']).toarray()
data_X=vectorizer2.fit_transform(data[u'产品标题']).toarray()

'''
from sklearn.metrics import accuracy_score, log_loss
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC, LinearSVC, NuSVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import learning_curve
from sklearn.metrics import precision_score
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import recall_score

classifiers = [
    #KNeighborsClassifier(3),
    SVC(degree=3),
    LinearSVC(C=1.0),
    NuSVC(probability=True),
    DecisionTreeClassifier(),
    RandomForestClassifier(),
    AdaBoostClassifier(),
    GradientBoostingClassifier(random_state=75,learning_rate=0.1,max_depth=5,loss='exponential',min_samples_split=3,min_samples_leaf=3,subsample=0.5),
    GaussianNB(),
    LinearDiscriminantAnalysis(n_components=2,solver='lsqr', shrinkage='auto'),
    QuadraticDiscriminantAnalysis(),
    MLPClassifier(solver='lbfgs', alpha=0.0001,batch_size=200,activation='relu',hidden_layer_sizes=(5, 2), random_state=50)
]

# Logging for Visual Comparison
log_cols=["Classifier", "Accuracy", "Log Loss","precision","recall","precision_recall_curve"]
log = pd.DataFrame(columns=log_cols)

X_train, X_test, y_train, y_test = train_test_split(data_X, data_Y ,test_size=0.2, random_state=17)

for i,clf in enumerate(classifiers):
    clf.fit(X_train, y_train)
    name = clf.__class__.__name__
    
    print("="*30)
    print(name)
    
    print('****Results****')
    train_predictions = clf.predict(X_test)
    acc = accuracy_score(y_test, train_predictions)
    print("Accuracy: {:.4%}".format(acc))
    
    train_predictions = clf.predict(X_test)
    ll = log_loss(y_test, train_predictions)
    print("Log Loss: {}".format(ll))
    
    pre=precision_score(y_test, train_predictions)
    print("precision_score: {}".format(pre))

    rec=recall_score(y_test, train_predictions)
    print("recall_score: {}".format(rec))

    sco=precision_recall_curve(y_test, train_predictions)
    print("precision_recall_curve: {}".format(sco))


    val=2*rec*pre/(rec+pre)
    print("value:{}".format(str(val)))

    log_entry = pd.DataFrame([[name, acc*100, ll,pre,rec,sco]], columns=log_cols)
    log = log.append(log_entry)

    
print("="*30)
'''