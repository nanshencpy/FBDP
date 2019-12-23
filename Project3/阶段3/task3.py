#%%
#buyer,goods,category,seller,brand,month,day,action,age,sex,province
#买家id,商品id,商品类别id,卖家id,品牌id,交易月,交易日,行为,买家年龄分段,性别,收货地址省份
from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName('my_app_name').getOrCreate()
df = spark.read.csv('million_user_log.csv', header=True, inferSchema=True)
df.createGlobalTempView("test")
#%%
# 阶段一 统计各省销售最好的产品类别前十（销售最多前10的产品类别）
spark.sql("SELECT count(*) as num, category, province FROM global_temp.test WHERE action = '2' GROUP BY province, category ORDER BY province, num DESC").show()
#%%
# 或者
t = df.filter(df['action'] == 2).groupBy('category', 'province').count().sort('province', 'count', ascending=False)
def fun(values):
    m = []
    for item in values:
        m = m + [item]
    return m[: 10]
rdd = t.rdd
pairs = rdd.map(lambda x: (x['province'], x['category']))
pairsR = pairs.groupByKey().mapValues(fun)
pairsR.collect()
#%%
# 阶段二 统计各省的双十一前十热门销售产品（购买最多前10的产品）-- 和MapReduce作业对比结果
spark.sql("SELECT count(*) as num, goods, province FROM global_temp.test WHERE action = '2' GROUP BY province, goods ORDER BY province, num DESC")
#%%
# 或者
t = df.filter(df['action'] == 2).groupBy('goods', 'province').count().sort('province', 'count', ascending=False)
def fun(values):
    m = []
    for item in values:
        m = m + [item]
    return m[: 10]
rdd = t.rdd
pairs = rdd.map(lambda x: (x['province'], x['goods']))
pairsR = pairs.groupByKey().mapValues(fun)
pairsR.collect()
#%%
# 阶段三：查询双11那天浏览次数前十的品牌 -- 和Hive作业对比结果
t = df.filter(df['action'] == 0).groupBy('brand').count().sort('count', ascending=False).show(10)
# 或者
spark.sql("SELECT count(*) as num, brand as brd FROM global_temp.test WHERE action = '0' GROUP BY brand ORDER BY num DESC").show(10)