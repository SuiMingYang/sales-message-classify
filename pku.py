import pkuseg

seg = pkuseg.pkuseg()           # 以默认配置加载模型
text = seg.cut('今天天气不错')  # 进行分词
print(text)
