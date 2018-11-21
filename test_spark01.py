
import sys
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

#读取log_test.txt文件内容，存入RDD
print("读取log_test.txt文件内容，存入RDD------------")
lines=sc.textFile('log_test.txt')
print("文件内容是：",lines.count())
#print("文件内容是：",lines.filter(lambda line:"Python" in line))
print("文件内容是：",lines.collect())

"""
PythonRDD=lines.filter(lambda x:"Python" in x)
print("Python是---:",PythonRDD.collect())


number=sc.textFile("demo_test.txt")
number1 = sc.parallelize([1,2,3,4,5])
sum1 = number1.reduce(lambda x,y:x+y)
print(sum1)
"""

'''
number2 = number.map(lambda x:x+x)
print(number2.collect())

number3 = sc.parallelize([1,2,3,4])
print(number3.map(lambda x:x+x).collect())
'''

print("-------------------")
nums=sc.parallelize([1,2,3,4,5,6,7],3)
#nums1=nums.map(lambda x: (x[0]+ x[1]))
nums1=nums.map(lambda x: x+1)
print(nums1.collect())



# parallelize：并行化数据，转化为RDD
print("并行化数据，转化为RDD-------------")
rdd = sc.parallelize([1,2,3,4,5], 3)
def f(iterator): yield sum(iterator)
print(rdd.mapPartitions(f).collect())

x = sc.parallelize([("a", 1), ("b", 4),("c",7)])
y = sc.parallelize([("a", 2), ("a", 3)])
'''
print(x.join(y).collect())
print(x.collect(),y.collect())
'''
print(x.first())

#map迭代操作RDD
print("map迭代操作RDD---------------------------------------------------------")
datalist=[1,2,3,4,5,6]
dataRDD=sc.parallelize(datalist)
resultRDD=dataRDD.map(lambda x:(x,x))
print(resultRDD.collect())


def my_add(x):
    return (x,x)
data = [1, 2, 3, 4, 5,6]
distData = sc.parallelize(data)  # 并行化数据集
result = distData.map(my_add)
print (result.collect()) 

#filter过滤操作
print("filter过滤操作-------------------------------------------------")
filterRDD=dataRDD.filter(lambda x:x>2)
print("filter result is:",filterRDD.collect())

# zip：将两个RDD对应元素组合为元组
print("zip操作----------------------------------------------------------")
x = sc.parallelize(range(0,5))
y = sc.parallelize(range(1000, 1005))
print(x.collect())
print (x.zip(y).collect())

#union 组合两个RDD为一个RDD
print (x.union(y).collect())

#reduce操作
print("reduce操作---------------------------------------------------------")
reduceRDD=sc.parallelize([1,2,3,4,5,6])
reduceResult=reduceRDD.reduce(lambda x,y:x+y)
print(reduceResult)


# groupby函数：根据提供的方法为RDD分组：
print("groupby操作--------------------------------------------------")
rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
def fun(i):
    return i % 2
result = rdd.groupBy(fun).collect()

print ([(x, sorted(y)) for (x, y) in result])


print("count:",reduceRDD.count())
print("sum:",reduceRDD.sum())
print("min:",reduceRDD.min())
print("name:",reduceRDD.name())
print("take(3):",reduceRDD.take(3))






