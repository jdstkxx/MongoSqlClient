# MongoSqlClient  
SQL-to-MongoDB translator library that let's you query MongoDB using plain SQL.  
  
  
//  MongoSqlClient  C# 5.0 兼容完整版    
//  封装类：MongoSqlHelper  
//  说明：数据库连接参数读取config文件  
//  依赖：log4.net  
  
/*     
使用说明 ：  
1、安装mongodb 3.6及以上版本，确保启动并连接成功。  
2、修改web.config中有关 MongoConn 连接字符串配置，改成你的mongodb的账号、密码  
*/  
  
// 版本依赖：MongoDB 驱动 2.11.5 + MongoDB 3.6、4.2 + C# 5.0 + .NET 4.5.2     
/*  语法支持情况    
 一、已支持（✅）—— 3.6 保证能跑  
  1. 等值/范围/IN 过滤  
     SELECT * FROM tbl WHERE status=1 AND price>100 AND city IN ('bj','sh')  
  2. TOP    
     SELECT TOP 50 * FROM tbl WHERE uid=1 ORDER BY createTime DESC  
  3. COUNT / SUM / MAX / MIN / AVG  
     SELECT COUNT(*) FROM tbl WHERE uid=1  
     SELECT SUM(amount) AS total FROM tbl WHERE uid=1  
     *****只允许单个查询，不允许同时查count 和max，要同时查多个聚合，请分别提交，譬如：****  
     "SELECT COUNT(*),MAX(price) FROM tbl"   --- 不支持  
  4. DISTINCT  
     SELECT DISTINCT uid FROM tbl  
  5. 一次插入多行  
     INSERT INTO tbl(a,b) VALUES (1,'a'),(2,'b')  
  7. 参数化写法（仅支持“拼好字符串”模式，不再传字典）：  
     string sql = "SELECT * FROM tbl WHERE uid=1 AND status=2";   // 直接拼  
     DataTable dt = MongoSqlHelper.Select(sql);  
  
 二、不支持（❌）—— 3.6 根本没有，永远跑不通  
  · JOIN / 子查询 / UNION / 事务 / HAVING / 行锁  
  · 表达式索引（如 $toInt:field）—— 3.6 不支持，必须预存字段再建索引  
  · OFFSET / SKIP + LIMIT 组合（驱动支持，但本类未暴露 SKIP）  
  · 视图、存储过程、触发器  
  . CAST（驱动 2.11.5 自动用 $toInt/$toDate，但 3.6 无表达式索引，仅聚合场景,）  
     SELECT CAST(price AS INT) AS priceInt FROM tbl ORDER BY CAST(price AS INT) ASC  
     —— 会走聚合管道，**无索引**，大表慎用，所以也取消支持。  
  
 三、能跑但会全表扫（⚠️）—— 数据量大时禁止  
  · LIKE '%xx%'   →  MongoDB $regex，3.6 不支持索引  
  · 对 CAST/计算字段排序/过滤   →  走聚合，无索引  
  
 四、版本红线（🔴）  
  · 驱动 2.11.5 最低要求 MongoDB 2.6，已测试 3.6+  
  · .NET Framework 不得低于 4.5.2（驱动硬性要求）  
  · C# 5.0 无 await/async，本类全部同步接口，无异步版本  
  
 五、性能锦囊  
  1. 凡是用于 WHERE / ORDER BY 的字段，**务必预存为纯类型**并建索引：  
        db.col.updateMany({},[{$set:{priceInt:{$toInt:"$price"}}}])  
        db.col.createIndex({priceInt:1})  
  2. 日志级别调至 DEBUG 可在 log4net 中看到 Compass 可直接粘贴的执行脚本  
  3. 内存缓存限额在 app.config 配置（已加 MongoSql 100 MB）  
 */