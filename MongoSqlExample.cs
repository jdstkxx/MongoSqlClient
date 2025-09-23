// ===================================================================================
//  MongoSqlClient  C# 5.0 示例程序
// ===================================================================================

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Text;
using log4net;
using log4net.Core;
using log4net.Appender;
using log4net.Layout;
using log4net.Config;
using log4net.Repository.Hierarchy;


namespace MongoSql.Demo
{
    internal class Program
    {
        private static string table="mongosql_table";

        private static void Main(string[] args)
        {
        	/* 1. 设置日志输出到控制台 */
        	//BasicConfigurator.Configure();
        	
        	/* 2. 设置日志输出到Log.txt */
	        // 获取当前日志库的根日志器
	        var hierarchy = (Hierarchy)LogManager.GetRepository();	
	        // 创建一个文件Appender
	        var fileAppender = new FileAppender
	        {
	            File = "Logs/Log.txt", // 指定日志文件路径
	            AppendToFile = true,  // 是否追加到文件
	            Layout = new PatternLayout("%date [%thread] %-5level %logger - %message%newline"), // 日志格式
	            Threshold = Level.Info, // 设置日志级别为 INFO
	            Encoding = Encoding.UTF8 // 设置文件编码为 UTF-8
	        };
	        fileAppender.ActivateOptions(); // 激活Appender	
	        // 将文件Appender添加到根日志器
	        hierarchy.Root.AddAppender(fileAppender);	
	        // 设置根日志器的级别为 INFO
	        hierarchy.Root.Level = Level.Info;

            /* 3. 自动初始化一些数据（不硬编码） */
            InitDemoData();
            

            /* 4. 键盘交互菜单 */
            string cmd;
            Console.WriteLine("=== MongoSql 交互演示 ===");
            while (true)
            {
                Console.WriteLine("\n【1】查询  【2】插入  【3】更新  【4】删除  【5】统计 【6】查询单行单列 【7】查询单列 【0】退出");
                Console.Write("请输入操作编号：");
                cmd = Console.ReadLine();
                try
                {
                    switch (cmd)
                    {
                        case "1": Query(); break;
                        case "2": Insert(); break;
                        case "3": Update(); break;
                        case "4": Delete(); break;
                        case "5": Count(); break;
                        case "6": GetColValueSingle(); break;
                        case "7": GetColValueMulti(); break;                        
                        case "0": return;
                        default: Console.WriteLine("无效编号"); break;
                    }
                }
                catch (MongoSqlException mex)
                {
                    Console.WriteLine("⚠️ SQL 错误：" + mex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("⚠️ 异常：" + ex.Message);
                }
            }
        }

        #region 初始化演示数据（零硬编码）
        private static void InitDemoData()
        {
            Console.Write("首次运行，是否插入演示数据？(y/n 默认y)：");
            if (Console.ReadLine().ToLower() == "n") return;

            /* 完全动态字段，实体仅做文档 */
            var sql = string.Format(
                "INSERT INTO {0}(Name,Price,Stock,CreateTime) VALUES " +
                "('键盘',99,200,'{1}')," +
                "('鼠标',59,150,'{1}')," +
                "('显示器',999,30,'{1}')",
                table, DateTime.Now.ToString("yyyy-MM-dd"));

            int n = MongoSqlHelper.Insert(sql);
            Console.WriteLine("已插入 {0} 条演示数据", n);
        }
        #endregion

        #region 各操作（均读键盘，零硬编码）
        private static void Query()
        {
            Console.Write("请输入 WHERE 子句（直接回车=全表）：");
            string where = Console.ReadLine();
            Console.Write("请输入 ORDER BY 子句（直接回车=不排序）：");
            string order = Console.ReadLine();
            Console.Write("请输入 TOP 数量（直接回车=不限）：");
            string topStr = Console.ReadLine();
            int? top = null;
            if (!string.IsNullOrWhiteSpace(topStr)) top = int.Parse(topStr);

            /* 动态拼接 SELECT */
            string sql = string.Format("SELECT {0} * FROM {1} {2} {3}",
                                       top.HasValue ? "TOP " + top.Value : "",
                                       table,
                                       string.IsNullOrWhiteSpace(where) ? "" : "WHERE " + where,
                                       string.IsNullOrWhiteSpace(order) ? "" : "ORDER BY " + order);

            DataTable dt = MongoSqlHelper.Select(sql);
            if (dt.Rows.Count == 0)
            {
                Console.WriteLine("（无数据）");
                return;
            }

            /* 动态打印表头、行 */
            foreach (DataColumn c in dt.Columns) Console.Write(c.ColumnName + "\t");
            Console.WriteLine();
            foreach (DataRow r in dt.Rows)
            {
                foreach (DataColumn c in dt.Columns) Console.Write(r[c] + "\t");
                Console.WriteLine();
            }
        }

        private static void Insert()
        {
            /* 支持动态列数 */
            Console.Write("请输入列名，用逗号分隔（例：Name,Price,Stock）：");
            string cols = Console.ReadLine();
            Console.Write("请输入 VALUES 子句（例：('键盘',99,200),('鼠标',59,150)）：");
            string vals = Console.ReadLine();

            string sql = string.Format("INSERT INTO {0}({1}) VALUES {2}",
                                       table, cols, vals);
            int n = MongoSqlHelper.Insert(sql);
            Console.WriteLine("成功插入 {0} 条", n);
        }

        private static void Update()
        {
            Console.Write("请输入 SET 子句（例：Price=88,Stock=300）：");
            string set = Console.ReadLine();
            Console.Write("请输入 WHERE 子句（例：Name='键盘'）：");
            string where = Console.ReadLine();

            string sql = string.Format("UPDATE {0} SET {1} WHERE {2}",
                                       table, set, where);
            int n = MongoSqlHelper.Update(sql);
            Console.WriteLine("成功更新 {0} 条", n);
        }

        private static void Delete()
        {
            Console.Write("请输入 WHERE 子句（例：Price<60）：");
            string where = Console.ReadLine();

            string sql = string.Format("DELETE FROM {0} WHERE {1}", table, where);
            int n = MongoSqlHelper.Delete(sql);
            Console.WriteLine("成功删除 {0} 条", n);
        }

        private static void Count()
        {
            Console.Write("请输入 WHERE 子句（直接回车=全表）：");
            string where = Console.ReadLine();
            string sql = string.Format("SELECT COUNT(*) FROM {0} {1}",
                                       table,
                                       string.IsNullOrWhiteSpace(where) ? "" : "WHERE " + where);
            long c = MongoSqlHelper.Count(sql);
            Console.WriteLine("满足条件记录数：" + c);
        }
        
        private static void GetColValueSingle()
        {
        	
        	/* 列 */
            Console.Write("请输入列名，用逗号分隔（例：Name,Price,Stock）：");
            string col = Console.ReadLine();
            Console.Write("请输入 WHERE 子句（直接回车=取第一行）：");
            string where = Console.ReadLine();
            string sql = string.Format("SELECT {0} FROM  {1} {2}",
                                       col, table,
                                       string.IsNullOrWhiteSpace(where) ? "" : "WHERE " + where);
            var obj = MongoSqlHelper.ExecuteGetSingle(sql);
            Console.WriteLine("获取到字段{0}值 {1} ", col,obj);
        }
        
        private static void GetColValueMulti()
        {
        	        	/* 列 */
            Console.Write("请输入列名，用逗号分隔（例：Name,Price,Stock）：");
            string col = Console.ReadLine();
            Console.Write("请输入 WHERE 子句（直接回车=全表）：");
            string where = Console.ReadLine();            
            string sql = string.Format("SELECT {0} FROM  {1} {2}",
                                       col, table,
                                       string.IsNullOrWhiteSpace(where) ? "" : "WHERE " + where);
            List<object> dlists = MongoSqlHelper.ExecuteGetList(sql);
            foreach (var val in dlists)
            {
                Console.Write(val + "\t");
                Console.WriteLine();
            }
        }
                
        #endregion
    }
}