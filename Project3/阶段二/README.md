#### 阶段二任务（Hive）：

Hive操作
* 把精简数据集导入到数据仓库Hive中
* 查询双11那天有多少人购买了商品
```
SELECT count(DISTINCT userid) as num FROM test WHERE action = '2';
```
输出37202
* 查询双11那天男女买家购买商品的比例
```
SELECT count(gender) as num FROM test WHERE action = '2' and gender = '0';
SELECT count(gender) as num FROM test WHERE action = '2' and gender = '1';
```
分别输出39058和38932
* 查询双11那天浏览次数前十的品牌
```
SELECT count(*) as num brandid as bid FROM test WHERE action = '0' GROUP BY brandid ORDER BY num DESC;
```

|数量|品牌|
|----|----|
|49151|1360|
|10130|3738|
|9719|82|
|9426|1446|
|8568|6215|
|8470|1214|
|8282|5376|
|7990|2276|
|7808|1662|
|7661|8235|
