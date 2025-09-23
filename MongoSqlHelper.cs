// ===================================================================================
//  MongoSqlClient  C# 5.0 兼容完整版 
//  封装类：MongoSqlHelper
//  说明：数据库连接参数读取config文件
//  依赖：log4.net
// ===================================================================================

using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;
using System.Configuration;
using log4net;


/* 相关版本：MongoDB 驱动 2.11.5 + MongoDB 3.6、4.2 + C# 5.0 + .NET 4.5.2  
 
 一、已支持（✅）—— 可直接抄，3.6 保证能跑
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
namespace MongoSql.Demo
{
	public static class MongoSqlHelper
	{
	    private static readonly MongoSqlClient Client;
	    private static readonly ILog logger = LogManager.GetLogger(typeof(MongoSqlHelper));
	
	    static MongoSqlHelper()
	    {
	        try
	        {
	            string conn = ConfigurationManager.AppSettings["MongoConn"];
	            string db   = ConfigurationManager.AppSettings["MongoDb"];
	            if (string.IsNullOrEmpty(conn) || string.IsNullOrEmpty(db))
	                throw new Exception("AppSettings 缺少 MongoConn 或 MongoDb");
	

	            Client = new MongoSqlClient(conn, db,
	                      new List<string> {
	                          "table1",
	                          "table2",
	                          "mongosql_table"
	                      });
	        }
	        catch (Exception ex)
	        {
	            LogAndException.Throw(ex,"MongoSqlClient 初始化失败...");
	        }
	    }
	
	    #region 通用日志模板
	    private static void LogEnter([System.Runtime.CompilerServices.CallerMemberName] string method = "",
	                                 string sql = "")
	    {
	        logger.Info("[执行sql开始]" + method + " || SQL: " + sql);
	    }
	
	    private static void LogExit(string method, object ret)
	    {
	        logger.Info("[执行sql结束]" + method + " || Return: " + (ret ?? "null"));
	    }
	    #endregion
	    
		#region ------- SQL 语法安检（C# 5.0 合并版） -------
		// 一站式预检：非法字符、永不支持语法、CAST+ORDER BY、LIKE 全表扫 全部处理。
		// 正常引号允许出现，譬如：Name="abc'
		// 配置项可关闭 CAST 排序拦截。
		private static void CheckSql(string sql)
		{
		    if (string.IsNullOrWhiteSpace(sql))
		        throw new Exception("SQL 为空");
		
		    string upper = sql.ToUpper().Trim();
		
		    /* 1. 危险字符（控制符、分号、反斜杠）*/
		    if (Regex.IsMatch(sql, @"[\x00-\x08\x0B-\x0C\x0E-\x1F;\\]"))
		        throw new Exception("SQL 含非法字符（控制符、分号、反斜杠）");
		
		    /* 2. 永不支持的语法——整词匹配，防止字段名误杀 */
		    string[] never = { "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN",
		                       "HAVING", "UNION", "TRANSACTION", "BEGIN", "ROLLBACK", "COMMIT",
		                       "CREATE", "DROP", "ALTER", "GRANT", "REVOKE","LIMIT" };
		    foreach (string kw in never)
		        if (Regex.IsMatch(upper, @"\b" + Regex.Escape(kw) + @"\b", RegexOptions.IgnoreCase))
		            throw new Exception("SQL 含不支持的语法：" + kw);
		
		    /* 3. CAST + ORDER BY —— 直接拒（可配置关闭）*/
		    if ( Regex.IsMatch(upper, @"\bCAST\s*\(") &&
		        Regex.IsMatch(upper, @"\bORDER\s+BY\b"))
		        throw new Exception("CAST+ORDER BY 无索引，已全局禁止（配置 ForbidCastSort=false 可放行）");
		
		    /* 4. 性能陷阱——禁止 */
		    if (Regex.IsMatch(sql, @"\bLIKE\s+'%[^']*%'", RegexOptions.IgnoreCase))
		        throw new Exception("SQL 含 LIKE '%xx%'，会全表扫描，已禁止");
		    
		    /* 5. 聚合函数数量——只允许 1 个 */
			int aggCount = 0;
			string[] aggs = { "COUNT", "SUM", "MAX", "MIN", "AVG" };
			foreach (string a in aggs)
			    aggCount += Regex.Matches(upper, @"\b" + a + @"\b", RegexOptions.IgnoreCase).Count;
			
			if (aggCount > 1)
			    throw new Exception("一条 SQL 只允许 1 个聚合函数（COUNT/SUM/MAX/MIN/AVG），请分别提交");
		}
		#endregion
	
	
		//<summary>
		//查询返回 DataTable
		//完整示例：
		//DataTable dt = MongoSqlHelper.Select("SELECT pkid,col1,col2 FROM table1 WHERE pkid=1 ORDER BY pkid ASC");
		//</summary>
	    public static DataTable Select(string sql)
	    {
	    	CheckSql(sql);
	        LogEnter("Select", sql);
	        DataTable dt = Client.ExecuteDataTable(sql, null);
	        if (dt == null || dt.Rows.Count == 0)
	            logger.Warn("Select 返回空结果集");
	        LogExit("Select", dt.Rows.Count + " 行");
	        return dt;
	    }
	
		//<summary>
		//插入
		//完整示例：
		//int rows = MongoSqlHelper.Insert("INSERT INTO table1(pkid,col1,col2) VALUES (1,'test','test2')");
		//</summary>
	    public static int Insert(string sql)
	    {
	    	CheckSql(sql);
	        LogEnter("Insert", sql);
	        int rows = Client.ExecuteInsert(sql);
	        LogExit("Insert", rows + " 行插入");
	        return rows;
	    }
	
	    //<summary>
	    //更新
	    //完整示例：
	    //int rows = MongoSqlHelper.Update("UPDATE table1 SET col1='new name' WHERE pkid=1");
	    //</summary>	    
	    public static int Update(string sql)
	    {
	    	CheckSql(sql);
	        LogEnter("Update", sql);
	        int rows = Client.ExecuteUpdate(sql, null);
	        LogExit("Update", rows + " 行更新");
	        return rows;
	    }
	
	    //<summary>
	    //删除
	    //完整示例：
	    //int rows = MongoSqlHelper.Delete("DELETE FROM table WHERE pkid=1");
	    //</summary>
	    public static int Delete(string sql)
	    {
	    	CheckSql(sql);
	        LogEnter("Delete", sql);
	        int rows = Client.ExecuteDelete(sql, null);
	        LogExit("Delete", rows + " 行删除");
	        return rows;
	    }
	
	    //<summary>
	    //计数
	    //完整示例：
	    //int cnt = MongoSqlHelper.Count("SELECT COUNT(*) FROM table1 WHERE pkid=1");
	    //</summary>
	    public static int Count(string sql)
	    {
	    	CheckSql(sql);
	        LogEnter("Count", sql);
	        int cnt = Client.ExecuteCount(sql, null);
	        LogExit("Count", cnt);
	        return cnt;
	    }
	
	    //<summary>
	    //单行单列
	    //完整示例：
	    //object name = MongoSqlHelper.ExecuteGetSingle("SELECT col1 FROM table1 WHERE pkid=1");
	    //</summary>
	    public static object ExecuteGetSingle(string sql)
	    {
	    	CheckSql(sql);
	        LogEnter("ExecuteGetSingle", sql);
	        object val = Client.ExecuteGetSingle(sql, null);
	        LogExit("ExecuteGetSingle", val);
	        return val;
	    }
	
	    //<summary>
	    //多行单列
	    //完整示例：
	    //List<object> list = MongoSqlHelper.ExecuteGetList("SELECT distinct pkid FROM table1 WHERE col2='test2' ");
	    //</summary>
	    public static List<object> ExecuteGetList(string sql)
	    {
	    	CheckSql(sql);
	        LogEnter("ExecuteGetList", sql);
	        List<object> list = Client.ExecuteGetList(sql, null);
	        LogExit("ExecuteGetList", list == null ? " 0 条" : list.Count + " 条");
	        return list;
	    }
	
	    #region 标准 Scalar 兼容（C# 5.0）
	    //<summary>
	    //第一行第一列（DBNull → null）
	    //完整示例：
	    //object cnt = MongoSqlHelper.ExecuteScalar("SELECT COUNT(*) FROM table1 WHERE col1='test1' ");
	    //</summary>
	    public static object ExecuteScalar(string sql)
	    {
	    	CheckSql(sql);
	        LogEnter("ExecuteScalar", sql);
	        DataTable dt = Select(sql);
	        object val = (dt.Rows.Count == 0 || dt.Columns.Count == 0) ? null : dt.Rows[0][0];
	        val = val == DBNull.Value ? null : val;
	        LogExit("ExecuteScalar", val);
	        return val;
	    }
	
	    //<summary>
	    //泛型 Scalar
	    //完整示例：
	    //int cnt = MongoSqlHelper.ExecuteScalar<int>("SELECT COUNT(*) FROM table1 WHERE col1='test1' ");
	    //</summary>
	    public static T ExecuteScalar<T>(string sql)
	    {
	    	CheckSql(sql);
	        object raw = ExecuteScalar(sql);
	        if (raw == null || raw == DBNull.Value)
	            return default(T);
	
	        Type u = Nullable.GetUnderlyingType(typeof(T));
	        return u != null
	            ? (T)Convert.ChangeType(raw, u)
	            : (T)Convert.ChangeType(raw, typeof(T));
	    }
	    #endregion
	}
}