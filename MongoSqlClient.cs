// ===================================================================================
//  MongoSqlClient  C# 5.0 兼容完整版  
//  基础类：MongoSqlClient  
//  说明：  含 Compass 可直接粘贴的执行脚本日志（参数化脱敏）
//  依赖：MongoDB.Bson ,MongoDB.Driver,MongoDB.Driver.Core,Newtonsoft.Json,log4.net
// ===================================================================================
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Runtime.Caching;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization;
using Newtonsoft.Json;
using log4net;

namespace MongoSql.Demo
{
    public sealed class MongoSqlClient : IDisposable
    {
        #region ── 私有字段 ──
        private readonly IMongoDatabase _db;
        private HashSet<string> _allowedTables;
        private static List<string> _collectionNames;
        private static readonly object _cacheLock = new object();
        private static DateTime _cacheExpires = DateTime.MinValue;
        private static readonly IBsonSerializer<BsonDocument> _docSerializer =
            BsonSerializer.SerializerRegistry.GetSerializer<BsonDocument>();

        private bool _disposed;
        private static readonly MemoryCache _parseCache = new MemoryCache("MongoSql");
        private static readonly ILog logger = LogManager.GetLogger(typeof(MongoSqlClient));
        #endregion

        #region ── 构造 / 释放 ──
        public MongoSqlClient(string connStr, string dbName, IEnumerable<string> allowedTables = null)
        {
            if (connStr == null) LogAndException.Throw(new ArgumentNullException("connStr"));
            if (dbName == null) LogAndException.Throw(new ArgumentNullException("dbName"));

            _db = new MongoClient(connStr).GetDatabase(dbName);

            _allowedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "table1",
                "table2"
            };

            if (allowedTables != null)
                _allowedTables = new HashSet<string>(allowedTables, StringComparer.OrdinalIgnoreCase);

            logger.Debug("表白名单：" + string.Join(", ", _allowedTables));
        }

        public void Dispose()
        {
            if (!_disposed) _disposed = true;
        }
        #endregion

        #region ── 表名校验 ──
        private void CheckTable(string table)
        {
            if (!_allowedTables.Contains(table))
                LogAndException.Throw(new Exception(string.Format("表 [{0}] 不在白名单内", table)));

            EnsureCollectionNamesUpToDate();
            if (!_collectionNames.Contains(table))
                //LogAndException.Throw(new Exception(string.Format("表 [{0}] 不存在于数据库中", table)));
            	logger.Debug(string.Format("表 [{0}] 不存在于数据库中", table));
        }

        private void EnsureCollectionNamesUpToDate()
        {
            lock (_cacheLock)
            {
                if (DateTime.UtcNow >= _cacheExpires)
                {
                    _collectionNames = _db.ListCollectionNames().ToList();
                    _cacheExpires = DateTime.UtcNow.AddHours(1);
                }
            }
        }
        #endregion

		#region ── 1. 查询 => DataTable ──
		public DataTable ExecuteDataTable(string sql, IDictionary<string, object> parameters = null)
		{
		    sql = CleanSql(sql);
		    logger.Debug("ExecuteDataTable入口 sql=" + sql);
		
		    try
		    {
		        var qi = CachedParse(sql, ParseSelect);
		        CheckTable(qi.Table);
		
		        if (qi.IsExists || qi.SubQuerySql != null)
		            return ExecuteExists(qi, parameters);
		
		        if (qi.IsGroupBy)
		            return ExecuteGroupBy(qi, parameters);
		
		        var coll = _db.GetCollection<BsonDocument>(qi.Table);
		        var filter = BuildFilterAuto(qi.Where, parameters);
		
		        /* ===== DISTINCT 去重 分支 ===== */
		        if (qi.IsDistinct)
		        {
		            var idDoc = new BsonDocument();
		            if (qi.Fields != null && qi.Fields.Length > 0)
		            {
		                foreach (var f in qi.Fields)
		                    idDoc[f] = "$" + f;
		            }
		            else
		            {
		                idDoc["$$ROOT"] = "$$ROOT";
		            }
		
		            var pipeline = new List<BsonDocument>();
		            if (filter != FilterDefinition<BsonDocument>.Empty)
		                pipeline.Add(new BsonDocument("$match", filter.ToBsonDocument()));
		            pipeline.Add(new BsonDocument("$group", new BsonDocument("_id", idDoc)));
		            pipeline.Add(new BsonDocument("$replaceRoot", new BsonDocument("newRoot", "$_id")));
		
		            var docs = coll.Aggregate<BsonDocument>(pipeline.ToArray()).ToList();
		
		            // 打印 Compass 可执行脚本
		            string pipelineJson = "[" + string.Join(", ", pipeline.Select(p => p.ToJson())) + "]";
		            logger.Info("ExecuteDataTable DISTINCT 脚本（可直接粘贴 Compass）:\r\n" +
		                         string.Format("db.{0}.aggregate({1})", qi.Table, pipelineJson));
		
		            logger.Debug("ExecuteDataTable DISTINCT 结果行数=" + docs.Count);
		            return ToDataTable(docs);
		        }
		        /* ============================== */
		
		        var find = coll.Find(filter);
		
		        bool isStar = qi.Fields != null && qi.Fields.Length == 1 && qi.Fields[0] == "*";
		        if (isStar)
		            find = find.Project(Builders<BsonDocument>.Projection.Exclude("_noSuchField"));
		        else if (qi.Fields != null && qi.Fields.Length > 0)
		        {
		            var proj = Builders<BsonDocument>.Projection;
		            ProjectionDefinition<BsonDocument> p = null;
		            foreach (var f in qi.Fields)
		                p = p == null ? proj.Include(f) : p.Include(f);
		            find = find.Project(p);
		        }
		
		        // 排序
		        BsonDocument sortDoc = null;
		        if (!string.IsNullOrEmpty(qi.OrderBy))
		        {
		            sortDoc = BuildSortAuto(qi.OrderBy);
		            find = find.Sort(sortDoc);
		        }
		        
		        //支持top语法，不支持limit		
		        if (qi.Top.HasValue && qi.Top.Value > 0)
		            find = find.Limit(qi.Top.Value);
		        
		        // 支持skip
				if (qi.Skip.HasValue && qi.Skip.Value > 0)
				    find = find.Skip(qi.Skip.Value);
		
		        // 构造 Compass 日志
		        string projectionClause;
		        if (isStar || (qi.Fields == null || qi.Fields.Length == 0))
		            projectionClause = "{}";
		        else
		            projectionClause = "{ " + string.Join(", ", qi.Fields.Select(f => "\"" + f + "\": 1")) + " }";
		
		        string sortClause = sortDoc == null ? "" : ".sort(" + sortDoc.ToJson() + ")";
		        string limitClause = qi.Top.HasValue ? ".limit(" + qi.Top.Value + ")" : "";
		
		        logger.Info("ExecuteDataTable 脚本（可直接粘贴 Compass）:\r\n" +
		                     string.Format("db.{0}.find({1}, {2}){3}{4}",
		                                   qi.Table,
		                                   filter.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToJson(),
		                                   projectionClause,
		                                   sortClause,
		                                   limitClause));
		
		        var list = find.ToList();
		        logger.Debug("ExecuteDataTable 返回Table行数=" + list.Count);
		        return ToDataTable(list);
		    }
		    catch (Exception ex)
		    {
		        LogAndException.Throw(ex);
		        return null; // 永远走不到
		    }
		}
		#endregion      
        
        #region ── 2. 插入 ──
        public int ExecuteInsert(string sql)
        {
            sql = CleanSql(sql);
            logger.Debug("ExecuteInsert sql=" + sql);

            try
            {
                var ii = CachedParse(sql, ParseInsertAuto);
                CheckTable(ii.Table);

                var docs = ii.Values.Select(row =>
                {
                    var doc = new BsonDocument();
                    for (int i = 0; i < ii.Columns.Count; i++)
                        doc[ii.Columns[i]] = BsonValue.Create(row[i]);
                    return doc;
                }).ToList();

                // 【补】打印 Compass 可执行脚本（参数化脱敏）
                string docsJson = "[" + string.Join(", ", docs.Select(d => d.ToJson())) + "]";
                logger.Info("ExecuteInsert 脚本（可直接粘贴 Compass）:\r\n" +
                             string.Format("db.{0}.insertMany({1})", ii.Table, docsJson));

                _db.GetCollection<BsonDocument>(ii.Table).InsertMany(docs);
                logger.Debug("返回ExecuteInsert 插入行数=" + docs.Count);
                return docs.Count;
            }
            catch (Exception ex)
            {
                LogAndException.Throw(ex);
                return 0;
            }
        }
        #endregion

        #region ── 3. 更新 ──
        public int ExecuteUpdate(string sql, IDictionary<string, object> parameters = null)
        {
            sql = CleanSql(sql);
            logger.Debug("ExecuteUpdate sql=" + sql);
            try
            {
                var ui = CachedParse(sql, ParseUpdateAuto);
                CheckTable(ui.Table);

                var coll = _db.GetCollection<BsonDocument>(ui.Table);
                var filter = BuildFilterAuto(ui.Where, parameters);
                var upd = Builders<BsonDocument>.Update;
                var sets = ui.SetList.Select(kv => upd.Set(kv.Key, BsonValue.Create(kv.Value))).ToList();

                // 【补】打印 Compass 可执行脚本
                string filterJson = filter.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToString();
                string updateJson = upd.Combine(sets).Render(_docSerializer, BsonSerializer.SerializerRegistry).ToString();
                logger.Info("ExecuteUpdate 脚本（可直接粘贴 Compass）:\r\n" +
                             string.Format("db.{0}.updateMany({1}, {2})",
                                           ui.Table, filterJson, updateJson));

                var result = coll.UpdateMany(filter, upd.Combine(sets));
                logger.Debug("返回ExecuteUpdate 修改行数=" + result.ModifiedCount);
                return (int)result.ModifiedCount;
            }
            catch (Exception ex)
            {
                LogAndException.Throw(ex);
                return 0;
            }
        }
        #endregion

        #region ── 4. 删除 ──
        public int ExecuteDelete(string sql, IDictionary<string, object> parameters = null)
        {
            sql = CleanSql(sql);
            logger.Debug("ExecuteDelete sql=" + sql);
            try
            {
                var di = CachedParse(sql, ParseDeleteAuto);
                CheckTable(di.Table);

                var coll = _db.GetCollection<BsonDocument>(di.Table);
                var filter = BuildFilterAuto(di.Where, parameters);

                // 【补】打印 Compass 可执行脚本
                string filterJson = filter.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToString();
                logger.Info("ExecuteDelete 脚本（可直接粘贴 Compass）:\r\n" +
                             string.Format("db.{0}.deleteMany({1})", di.Table, filterJson));

                var cnt = (int)coll.DeleteMany(filter).DeletedCount;
                logger.Debug("返回ExecuteDelete 删除行数=" + cnt);
                return cnt;
            }
            catch (Exception ex)
            {
                LogAndException.Throw(ex);
                return 0;
            }
        }
        #endregion

        #region ── 5. 统计 / 标量 ──
        public int ExecuteCount(string sql, IDictionary<string, object> parameters = null)
        {
            sql = CleanSql(sql);
            logger.Debug("ExecuteCount sql=" + sql);
            try
            {
                var qi = CachedParse(sql, ParseSelect);
                CheckTable(qi.Table);

                var coll = _db.GetCollection<BsonDocument>(qi.Table);
                var filter = BuildFilterAuto(qi.Where, parameters);

                // 【补】打印 Compass 可执行脚本
                string filterJson = filter.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToString();
                logger.Info("ExecuteCount 脚本（可直接粘贴 Compass）:\r\n" +
                             string.Format("db.{0}.countDocuments({1})", qi.Table, filterJson));

                int cnt = (int)coll.CountDocuments(filter);
                logger.Debug("返回ExecuteCount count行数=" + cnt);
                return cnt;
            }
            catch (Exception ex)
            {
                LogAndException.Throw(ex);
                return 0;
            }
        }
        #endregion

        #region ── 6. 单行单列查询 ──
        public string ExecuteGetSingle(string sql, IDictionary<string, object> parameters = null)
        {
            sql = CleanSql(sql);
            logger.Debug("ExecuteGetSingle sql=" + sql);

            try
            {
                var qi = CachedParse(sql, ParseSelect);
                CheckTable(qi.Table);

                if (qi.IsAggregate)
                {
                    object agg = ExecuteAggregate(qi, parameters);
                    string restr = agg == null ? null : agg.ToString();
                    logger.Debug("ExecuteGetSingle 带聚合查询，执行结果：" + restr);
                    return restr;
                }

                if (qi.Fields == null || qi.Fields.Length != 1)
                {
                    logger.Debug("字段数 ≠ 1");
                    throw new ArgumentException("只能查询单列");
                }

                var coll = _db.GetCollection<BsonDocument>(qi.Table);
                var filter = BuildFilterAuto(qi.Where, parameters);
                var proj = Builders<BsonDocument>.Projection.Include(qi.Fields[0]).Exclude("_id");

                // 【补】打印 Compass 可执行脚本
                string filterJson = filter.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToString();
                string projJson = proj.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToString();
                logger.Info("ExecuteGetSingle 脚本（可直接粘贴 Compass）:\r\n" +
                             string.Format("db.{0}.find({1}, {2}).limit(1)",
                                           qi.Table, filterJson, projJson));

                var doc = coll.Find(filter).Project(proj).Limit(1).FirstOrDefault();
                object value = SafeGetValue(doc, qi.Fields[0]);
                logger.Debug("返回 ExecuteGetSingle 单行单列查询结果=" + (value ?? "null"));
                return value == null ? null : value.ToString();
            }
            catch (Exception ex)
            {
                LogAndException.Throw(ex);
                return null;
            }
        }
        #endregion
        
        #region ── 6.1. 多行单列查询 ──
		public List<object> ExecuteGetList(string sql, IDictionary<string, object> parameters = null)
		{
		    sql = CleanSql(sql);
		    logger.Debug("ExecuteGetList 入口 sql=" + sql);
		
		    try
		    {
		        var qi = CachedParse(sql, ParseSelect);
		        CheckTable(qi.Table);
		
		        if (qi.Fields == null || qi.Fields.Length != 1)
		        {
		            logger.Debug("字段数 ≠ 1");
		            throw new ArgumentException("SQL 必须仅查询一个列，如 SELECT Name FROM ...");
		        }
		
		        var coll   = _db.GetCollection<BsonDocument>(qi.Table);
		        var filter = BuildFilterAuto(qi.Where, parameters);
		        var proj   = Builders<BsonDocument>.Projection.Include(qi.Fields[0]).Exclude("_id");
		        var find   = coll.Find(filter).Project(proj);
		
		        if (qi.Top.HasValue && qi.Top.Value > 0)
		            find = find.Limit(qi.Top.Value);
		       
		
		        List<object> reli = find.ToList()
		                                .Select(doc => BsonTypeMapper.MapToDotNetValue(doc.GetValue(qi.Fields[0])))
		                                .ToList();
		
		        //【补】打印 Compass 可执行脚本
		        string limitClause = qi.Top.HasValue && qi.Top.Value > 0
		                                 ? ".limit(" + qi.Top.Value + ")"
		                                 : "";
		        string filterJson  = filter.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToString();
		        string projJson    = proj.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToString();
		
		        logger.Info("ExecuteGetList 脚本（可直接粘贴 Compass）:\r\n" +
		                     string.Format("db.{0}.find({1}, {2}){3}",
		                                   qi.Table, filterJson, projJson, limitClause));
		
		        logger.Debug("返回 ExecuteGetList 单列多行行数=" + reli.Count);
		        return reli;
		    }
		    catch (Exception ex)
		    {
		        LogAndException.Throw(ex);
		        return null;   // 永远走不到
		    }
		}
		#endregion

        #region ── 7. 解析辅助类 ──
        private class Qi
        {
            public string Table;
            public string Where;
            public string OrderBy;
            public string[] Fields;
            public int? Top;
            public bool IsCount;
            public string CountAlias;
            public bool IsAggregate;
            public string AggFunc;
            public string AggField;
            public string AggAlias;
            public bool IsGroupBy;
            public string[] GroupFields;
            public string Having;
            public bool IsExists;
            public string SubQuerySql;
            public string SubQuerySelect;
            public string SubQueryForeign;
            public bool IsDistinct;
            public int? Skip;   // 新增 skip
        }

        private class InsertAuto
        {
            public string Table;
            public List<string> Columns;
            public List<List<object>> Values;
        }

        private class UpdateAuto
        {
            public string Table;
            public string Where;
            public Dictionary<string, object> SetList;
        }

        private class DeleteAuto
        {
            public string Table;
            public string Where;
        }
        #endregion

        #region ── 8. 正则模板 ──
        private static readonly Regex _cleanSpace =
            new Regex(@"\s+", RegexOptions.Compiled | RegexOptions.Multiline);

        private static readonly Regex _reSelect =
            new Regex(@"SELECT\s+(?:TOP\s+(?<top>\d+)\s+)?(?<fields>.+?)\s+FROM\s+(?<table>\w+)(?:\s+WHERE\s+(?<where>.*?))?(?:\s+ORDER\s+BY\s+(?<order>.*?))?$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

        private static readonly Regex _reInsert =
            new Regex(@"INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+)",
                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static readonly Regex _reUpdate =
            new Regex(@"UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

        private static readonly Regex _reDelete =
            new Regex(@"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

        private static readonly Regex KeyWordRegex =
            new Regex(@"\b(SELECT|FROM|WHERE|AND|OR|GROUP\s+BY|HAVING|EXISTS|NOT\s+EXISTS|ORDER\s+BY|TOP|AS|IN|LIKE|IS|NULL|ASC|DESC|BETWEEN|DISTINCT|SKIP|CAST)\b",
                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static readonly Regex ObjectIdRegex =
            new Regex(@"(\w+)\s*=\s*['""]([0-9a-fA-F]{24})['""]",
                RegexOptions.Compiled | RegexOptions.IgnoreCase);
        #endregion

        #region ── 9. 解析入口 ──
        private static string UpperKeyWords(string sql)
        {
            return KeyWordRegex.Replace(sql, delegate(Match m) { return m.Value.ToUpper(); });
        }

        private static string CleanSql(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;
            return _cleanSpace.Replace(sql, " ").Trim();
        }

        private static Qi ParseSelect(string sql)
        {
            sql = CleanSql(sql);
            logger.Debug(string.Format("[ParseSelect] 原始 SQL：{0}", sql));
            if (sql.Length > 5000) sql = sql.Substring(0, 5000);
            sql = UpperKeyWords(sql);

            bool isDistinct = Regex.IsMatch(sql, @"\bDISTINCT\b", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\bDISTINCT\b", "", RegexOptions.IgnoreCase);

            int? top = null;
            var topM = Regex.Match(sql, @"TOP\s+(\d+)", RegexOptions.IgnoreCase);
            if (topM.Success)
            {
                top = int.Parse(topM.Groups[1].Value);
                sql = sql.Remove(topM.Index, topM.Length);
            }
            
            // 已有 top 捕获之后追加
			int? skip = null;
			var skipM = Regex.Match(sql, @"SKIP\s+(\d+)", RegexOptions.IgnoreCase);
			if (skipM.Success)
			{
			    skip = int.Parse(skipM.Groups[1].Value);
			    sql = sql.Remove(skipM.Index, skipM.Length);   // 去掉关键字
			}

            var fieldM = Regex.Match(sql, @"SELECT\s+(.+?)\s+FROM", RegexOptions.IgnoreCase);
            if (!fieldM.Success)
                LogAndException.Throw(new MongoSqlException("找不到Select 字段列表 from table", sql));

            string rawFields = fieldM.Groups[1].Value.Trim();
            sql = sql.Remove(fieldM.Index, fieldM.Length).Insert(fieldM.Index, "SELECT  FROM");

            var tblM = Regex.Match(sql, @"FROM\s+(\w+)", RegexOptions.IgnoreCase);
            if (!tblM.Success)
                LogAndException.Throw(new MongoSqlException("找不到select from 表名", sql));
            string table = tblM.Groups[1].Value;

            string wheres = null;
            var whereM = Regex.Match(sql, @"WHERE\s+(.+?)(?:\s+ORDER\s+BY|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (whereM.Success) wheres = whereM.Groups[1].Value.Trim();

            string order = null;
            var ordM = Regex.Match(sql, @"ORDER\s+BY\s+(.+)$", RegexOptions.IgnoreCase);
            if (ordM.Success) order = ordM.Groups[1].Value.Trim();

            var qi = new Qi
            {
                Table = table,
                Where = wheres,
                OrderBy = order,
                Top = top,
                IsDistinct = isDistinct,
                Skip = skip
            };

            var countMatch = Regex.Match(rawFields, @"COUNT\s*\(\s*\*\s*\)(?:\s+AS\s+(\w+))?", RegexOptions.IgnoreCase);
            if (countMatch.Success)
            {
                qi.IsCount = true;
                qi.CountAlias = countMatch.Groups[1].Success ? countMatch.Groups[1].Value : "Count";
                qi.Fields = new[] { qi.CountAlias };
            }
            else
            {
                var aggMatch = Regex.Match(rawFields, @"\b(SUM|MAX|MIN|AVG)\s*\(\s*([^)]+)\s*\)(?:\s+AS\s+(\w+))?", RegexOptions.IgnoreCase);
                if (aggMatch.Success)
                {
                    qi.IsAggregate = true;
                    qi.AggFunc = aggMatch.Groups[1].Value.ToUpper();
                    qi.AggField = aggMatch.Groups[2].Value.Trim();
                    qi.AggAlias = aggMatch.Groups[3].Success ? aggMatch.Groups[3].Value : qi.AggFunc;
                    qi.Fields = new[] { qi.AggAlias };
                }
                else
                {
                    qi.Fields = rawFields.Split(',').Select(s => s.Trim()).ToArray();
                }
            }

            var groupM = Regex.Match(sql, @"GROUP\s+BY\s+([^H]+?)(?:\s+HAVING|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (groupM.Success)
            {
                qi.IsGroupBy = true;
                qi.GroupFields = groupM.Groups[1].Value.Split(',').Select(s => s.Trim()).ToArray();
            }
            var havM = Regex.Match(sql, @"HAVING\s+(.+)$", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (havM.Success) qi.Having = havM.Groups[1].Value.Trim();

            var existsM = Regex.Match(sql,
                @"\b(EXISTS|NOT\s+EXISTS)\s*\(\s*SELECT\s+\w+\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(\w+)\.(\w+)\s*\)",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (existsM.Success)
            {
                qi.IsExists = existsM.Groups[1].Value.ToUpper() == "EXISTS";
                qi.SubQuerySql = existsM.Groups[0].Value;
                qi.SubQuerySelect = existsM.Groups[3].Value;
                qi.SubQueryForeign = existsM.Groups[5].Value;
            }

            logger.Debug(string.Format(
                "[ParseSelect] 拆分成功 Table={0} Cols={1} Where={2} Order by={3} IsCount={4} IsAggregate={5} IsDistinct={6}",
                table, rawFields, wheres, order, qi.IsCount, qi.IsAggregate, qi.IsDistinct));
            return qi;
        }
        
        
		//<summary>
		//解析 INSERT 语句
		//</summary>
		private static InsertAuto ParseInsertAuto(string sql)
		{
		    sql = CleanSql(sql);
		    logger.Debug(string.Format("[ParseInsertAuto] 原始 SQL：{0}", sql));
		    sql = UpperKeyWords(sql);
		
		    var m = _reInsert.Match(sql);
		    if (!m.Success)
		        LogAndException.Throw(new MongoSqlException("INSERT 语法错误", sql));
		
		    var cols = m.Groups[2].Value.Split(',').Select(s => s.Trim()).ToList();
		    var valRows = Regex.Matches(m.Groups[3].Value, @"\(([^)]+)\)", RegexOptions.Compiled)
		                       .Cast<Match>()
		                       .Select(m2 => m2.Groups[1].Value.Split(',').Select(v => ParseValue(v.Trim())).ToList())
		                       .ToList();
		    var table = m.Groups[1].Value;
		
		    logger.Debug(string.Format("[ParseInsertAuto] 拆分成功 Table={0} Columns={1} 行数={2}",
		                               table, string.Join("|", cols), valRows.Count));
		
		    return new InsertAuto
		    {
		        Table   = table,
		        Columns = cols,
		        Values  = valRows
		    };
		}
		
		//<summary>
		//解析 UPDATE 语句
		//</summary>
		private static UpdateAuto ParseUpdateAuto(string sql)
		{
		    sql = CleanSql(sql);
		    logger.Debug(string.Format("[ParseUpdateAuto] 原始 SQL：{0}", sql));
		    sql = UpperKeyWords(sql);
		
		    var m = _reUpdate.Match(sql);
		    if (!m.Success)
		        LogAndException.Throw(new MongoSqlException("UPDATE 语法错误", sql));
		
		    var setDict = m.Groups[2].Value.Split(',')
		                   .Select(s => s.Split('='))
		                   .ToDictionary(a => a[0].Trim(), a => ParseValue(a[1].Trim()));
		
		    logger.Debug(string.Format("[ParseUpdateAuto] 拆分成功 Table={0} Set={1} Where={2}",
		                               m.Groups[1].Value,
		                               string.Join(";", setDict.Select(kv => kv.Key + "=" + kv.Value)),
		                               m.Groups[3].Value));
		
		    return new UpdateAuto
		    {
		        Table   = m.Groups[1].Value,
		        Where   = m.Groups[3].Value,
		        SetList = setDict
		    };
		}
		
		//<summary>
		//解析 DELETE 语句
		//</summary>
		private static DeleteAuto ParseDeleteAuto(string sql)
		{
		    sql = CleanSql(sql);
		    logger.Debug(string.Format("[ParseDeleteAuto] 原始 SQL：{0}", sql));
		    sql = UpperKeyWords(sql);
		
		    var m = _reDelete.Match(sql);
		    if (!m.Success)
		        LogAndException.Throw(new MongoSqlException("DELETE 语法错误", sql));
		
		    logger.Debug(string.Format("[ParseDeleteAuto] 拆分成功 Table={0} Where={1}",
		                               m.Groups[1].Value, m.Groups[2].Value));
		
		    return new DeleteAuto
		    {
		        Table = m.Groups[1].Value,
		        Where = m.Groups[2].Value
		    };
		}
        #endregion

        #region ── 14. 类型解析 ──
        private static object ParseValue(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return null;
            raw = raw.Trim();

            if ((raw.StartsWith("'") && raw.EndsWith("'")) ||
                (raw.StartsWith("\"") && raw.EndsWith("\"")))
                return raw.Substring(1, raw.Length - 2);

            if (raw.Equals("null", StringComparison.OrdinalIgnoreCase)) return null;

            bool b; if (bool.TryParse(raw, out b)) return b;

            if (raw.StartsWith("ObjectId(\"", StringComparison.OrdinalIgnoreCase) &&
                raw.EndsWith("\")") && raw.Length == 26)
                return new ObjectId(raw.Substring(10, 24));

            long l; if (Regex.IsMatch(raw, @"^-?\d+$") && long.TryParse(raw, out l)) return l;

            decimal d; if (Regex.IsMatch(raw, @"^-?\d+\.\d+$") && decimal.TryParse(raw, out d)) return d;

            DateTime dt; if (DateTime.TryParse(raw, out dt)) return dt;

            return raw;
        }
        #endregion

        #region ── 14.1 判断是否有效字段 ──
        //正则表达式来判断传入的字符串，是否符合编程规则
        private static readonly Regex FieldRegex = new Regex(@"^[a-zA-Z_]\w*$", RegexOptions.Compiled);

        private bool IsValidField(string field)
        {
            return FieldRegex.IsMatch(field);
        }
        #endregion

        #region ── 15. SQL→MongoDB 过滤器构造 ──        
        private FilterDefinition<BsonDocument> BuildFilterAuto(string where,
                                              IDictionary<string, object> paras,
                                              bool isHaving = false)
		{
		    if (string.IsNullOrEmpty(where)) return FilterDefinition<BsonDocument>.Empty;
		
		    where = ReplaceParameters(where, paras);
		
		    // 1. 空值保护：任何空串/缺失 → null（不抛异常）
		    where = Regex.Replace(where, @"(\w+)\s*=\s*$", "\"$1\":null");
		    where = Regex.Replace(where, @"(\w+)\s*=\s*''", "\"$1\":null");
		    where = Regex.Replace(where, @"(\w+)\s*=\s*""""", "\"$1\":null");
		
		    // 2. 运算符映射
		    where = Regex.Replace(where, @"(\w+)\s*>=\s*([^,\s}]+)", "\"$1\":{ \"$gte\" : $2 }");
		    where = Regex.Replace(where, @"(\w+)\s*<=\s*([^,\s}]+)", "\"$1\":{ \"$lte\" : $2 }");
		    where = Regex.Replace(where, @"(\w+)\s*!=\s*([^,\s}]+)", "\"$1\":{ \"$ne\"  : $2 }");
		    where = Regex.Replace(where, @"(\w+)\s*<\s*([^,\s}]+)", "\"$1\":{ \"$lt\"  : $2 }");
		    where = Regex.Replace(where, @"(\w+)\s*>\s*([^,\s}]+)", "\"$1\":{ \"$gt\"  : $2 }");
		    where = Regex.Replace(where, @"(\w+)\s*=\s*([^,\s}]+)", "\"$1\":$2");
		
		    // 3. 特殊操作
		    where = Regex.Replace(where, @"(\w+)\s+LIKE\s+'([^']+)'", "\"$1\":{ \"$regex\":\"$2\" }");
		    where = Regex.Replace(where, @"(\w+)\s+BETWEEN\s+([^'""\s]+)\s+AND\s+([^'""\s]+)",
		                        "\"$1\":{ \"$gte\":$2 , \"$lte\":$3 }", RegexOptions.IgnoreCase);
		
		    where = Regex.Replace(where, @"(\w+)\s+IN\s*\(([^)]+)\)", m =>
		    {
		        string field = m.Groups[1].Value;
		        string inner = m.Groups[2].Value;
		        var items = Regex.Matches(inner, @"['""]([^'""]*)['""]|([^,]+)")
		                         .Cast<Match>()
		                         .Select(x => x.Groups[1].Success
		                                           ? string.Format("\"{0}\"",
		                                               x.Groups[1].Value.Replace("\"", "\\\""))
		                                           : x.Groups[2].Value.Trim())
		                         .ToArray();
		        return string.Format("\"{0}\":{{ \"$in\" : [{1}] }}", field, string.Join(",", items));
		    }, RegexOptions.IgnoreCase);
		
		    where = Regex.Replace(where, @"(\w+)\s+IS\s+NULL", "\"$1\":{ \"$exists\" : false }", RegexOptions.IgnoreCase);
		
		    // 4. ObjectId
		    where = ObjectIdRegex.Replace(where, "\"$1\":ObjectId(\"$2\")");
		
		    // 5. 字段名加双引号
		    where = Regex.Replace(where, @"\{([a-zA-Z]\w*):", "{\"$1\":");
		    where = Regex.Replace(where, @"\s([a-zA-Z]\w*):", " \"$1\":");
		
		    if (isHaving)
		        where = Regex.Replace(where, @"\{""(\w+)"":", "{\"$$1\":");
		
		    // 6. AND / OR
		    if (Regex.IsMatch(where, @"\bAND\b", RegexOptions.IgnoreCase))
		    {
		        var parts = Regex.Split(where, @"\bAND\b", RegexOptions.IgnoreCase)
		                         .Select(p => "{" + p.Trim() + "}");
		        where = "{ \"$and\" : [" + string.Join(",", parts) + "] }";
		    }
		    else if (Regex.IsMatch(where, @"\bOR\b", RegexOptions.IgnoreCase))
		    {
		        var parts = Regex.Split(where, @"\bOR\b", RegexOptions.IgnoreCase)
		                         .Select(p => "{" + p.Trim() + "}");
		        where = "{ \"$or\" : [" + string.Join(",", parts) + "] }";
		    }
		    else
		    {
		        where = "{" + where + "}";
		    }
		
		    where = where.TrimEnd(' ', '\t', '\r', '\n', ';');		
		    logger.Debug("[BuildFilterAuto] 生成过滤器 JSON：" + where);

            try
            {
                var filter = BsonDocument.Parse(where);
                logger.Debug("[BuildFilterAuto] 过滤器 JSON 解析结果=" + filter.ToBsonDocument().ToString());
                return filter;
            }
            catch (Exception ex)
            {
                LogAndException.Throw(new MongoSqlException("过滤器 JSON 解析失败", where, ex));
                return null;
            }
		}
		#endregion

        #region ── 16. 聚合 / GROUP BY / EXISTS 执行 ──
        private object ExecuteAggregate(Qi qi, IDictionary<string, object> parameters)
        {
            logger.Debug("ExecuteAggregate开始，函数: " + qi.AggFunc);
            var coll = _db.GetCollection<BsonDocument>(qi.Table);
            var filter = BuildFilterAuto(qi.Where, parameters);

            string op;
            switch (qi.AggFunc)
            {
                case "SUM": op = "$sum"; break;
                case "MAX": op = "$max"; break;
                case "MIN": op = "$min"; break;
                case "AVG": op = "$avg"; break;
                default:
                    logger.Error("[ExecuteAggregate] 不支持的聚合函数：" + qi.AggFunc);
                    throw new MongoSqlException("不支持的聚合函数", qi.AggFunc);
            }

            var pipeline = new List<BsonDocument>
            {
                new BsonDocument("$match", filter.Render(_docSerializer, BsonSerializer.SerializerRegistry)),
                new BsonDocument("$group", new BsonDocument
                {
                    { "_id", 1 },
                    { qi.AggAlias, new BsonDocument(op, "$" + qi.AggField) }
                })
            };

            // 【补】打印 Compass 可执行脚本
            string pipelineJson = "[" + string.Join(", ", pipeline.Select(p => p.ToJson())) + "]";
            logger.Info("ExecuteAggregate 脚本（可直接粘贴 Compass）:\r\n" +
                         string.Format("db.{0}.aggregate({1})", qi.Table, pipelineJson));

            var result = coll.Aggregate<BsonDocument>(pipeline, new AggregateOptions()).FirstOrDefault();
            string value = result == null ? null : BsonTypeMapper.MapToDotNetValue(result[qi.AggAlias]).ToString();
            logger.Debug("返回 ExecuteAggregate 执行结果=" + (value ?? "null"));
            return value;
        }

        private DataTable ExecuteGroupBy(Qi qi, IDictionary<string, object> parameters)
        {
            logger.Debug("ExecuteGroupBy开始...");
            var coll = _db.GetCollection<BsonDocument>(qi.Table);
            var filter = BuildFilterAuto(qi.Where, parameters);

            var stages = new List<BsonDocument>
            {
                new BsonDocument("$match", filter.ToBsonDocument())
            };

            var groupId = new BsonDocument();
            foreach (var f in qi.GroupFields) groupId[f] = "$" + f;

            var accumulators = new BsonDocument { { "_id", groupId } };
            foreach (var field in qi.Fields)
            {
                if (qi.IsCount && field == qi.CountAlias)
                    accumulators[field] = new BsonDocument("$sum", 1);
                else if (qi.IsAggregate && field == qi.AggAlias)
                    accumulators[field] = new BsonDocument("$" + qi.AggFunc, "$" + qi.AggField);
                else if (Array.IndexOf(qi.GroupFields, field) >= 0)
                    accumulators[field] = new BsonDocument("$first", "$" + field);
            }
            stages.Add(new BsonDocument("$group", accumulators));

            if (!string.IsNullOrEmpty(qi.Having))
            {
                var havingBson = BuildFilterAuto(qi.Having, parameters, true);
                stages.Add(new BsonDocument("$match", havingBson.ToBsonDocument()));
            }

            if (!string.IsNullOrEmpty(qi.OrderBy))
            {
                var sortDoc = BsonDocument.Parse("{" + qi.OrderBy.Replace("DESC", "-1").Replace("ASC", "1") + "}");
                stages.Add(new BsonDocument("$sort", sortDoc));
            }

            if (qi.Top.HasValue && qi.Top.Value > 0)
                stages.Add(new BsonDocument("$limit", qi.Top.Value));

            // 【补】打印 Compass 可执行脚本
            string stagesJson = "[" + string.Join(", ", stages.Select(p => p.ToJson())) + "]";
            logger.Info("ExecuteGroupBy 脚本（可直接粘贴 Compass）:\r\n" +
                         string.Format("db.{0}.aggregate({1})", qi.Table, stagesJson));

            var list = coll.Aggregate<BsonDocument>(stages).ToList();
            logger.Debug("ExecuteGroupBy 返回行数=" + list.Count);
            return ToDataTable(list);
        }

        private DataTable ExecuteExists(Qi qi, IDictionary<string, object> parameters)
        {
            logger.Debug("ExecuteExists 开始...");
            var subQi = CachedParse(qi.SubQuerySql, ParseSelect);
            var subColl = _db.GetCollection<BsonDocument>(subQi.Table);
            var subFilter = BuildFilterAuto(subQi.Where, parameters);

            var fieldName = subQi.SubQuerySelect.ToLowerInvariant();
            var subList = subColl.Find(subFilter)
                                 .Project(Builders<BsonDocument>.Projection.Include(fieldName))
                                 .ToList()
                                 .Select(d => d.GetValue(fieldName))
                                 .Distinct()
                                 .ToList();

            var mainColl = _db.GetCollection<BsonDocument>(qi.Table);
            var mainFilter = BuildFilterAuto(qi.Where, parameters);

            if (subList.Any())
            {
                var inFilter = qi.IsExists
                    ? Builders<BsonDocument>.Filter.In(qi.SubQueryForeign, subList)
                    : Builders<BsonDocument>.Filter.Nin(qi.SubQueryForeign, subList);
                mainFilter = Builders<BsonDocument>.Filter.And(mainFilter, inFilter);
            }
            else
            {
                if (qi.IsExists)
                    mainFilter = Builders<BsonDocument>.Filter.And(mainFilter,
                        Builders<BsonDocument>.Filter.Eq("_id", ObjectId.Empty));
            }

            var find = mainColl.Find(mainFilter);

            bool isStar = qi.Fields != null && qi.Fields.Length == 1 && qi.Fields[0] == "*";
            if (isStar)
                find = find.Project(Builders<BsonDocument>.Projection.Exclude("_noSuchField"));
            else if (qi.Fields != null && qi.Fields.Length > 0)
            {
                var proj = Builders<BsonDocument>.Projection;
                ProjectionDefinition<BsonDocument> p = null;
                foreach (var f in qi.Fields)
                    p = p == null ? proj.Include(f) : p.Include(f);
                find = find.Project(p);
            }

            if (!string.IsNullOrEmpty(qi.OrderBy))
                find = find.Sort(BuildSortAuto(qi.OrderBy));

            if (qi.Top.HasValue && qi.Top.Value > 0)
                find = find.Limit(qi.Top.Value);

            // 【补】打印 Compass 可执行脚本
            string projectionClause = isStar ? "{}" 
                : "{ " + string.Join(", ", qi.Fields.Select(f => "\"" + f + "\": 1")) + " }";
            string sortClause = !string.IsNullOrEmpty(qi.OrderBy) 
                ? ".sort(" + BuildSortAuto(qi.OrderBy).ToJson() + ")" 
                : "";
            string limitClause = qi.Top.HasValue ? ".limit(" + qi.Top.Value + ")" : "";

            logger.Info("ExecuteExists 脚本（可直接粘贴 Compass）:\r\n" +
                         string.Format("db.{0}.find({1}, {2}){3}{4}",
                                       qi.Table,
                                       mainFilter.Render(_docSerializer, BsonSerializer.SerializerRegistry).ToJson(),
                                       projectionClause,
                                       sortClause,
                                       limitClause));

            var list = find.ToList();
            logger.Debug("ExecuteExists 返回行数=" + list.Count);
            return ToDataTable(list);
        }
        #endregion


		#region ── 17. 排序构造 ──
		private static readonly Regex CastRegex =
		    new Regex(@"\bCAST\s*\(\s*([^)]+)\s+AS\s+(INT|LONG|DECIMAL|DOUBLE|STRING|DATE|BOOL)\s*\)",
		        RegexOptions.IgnoreCase | RegexOptions.Compiled);
		
		private static readonly Dictionary<string, string> CastToMongo = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
		{
		    {"INT", "$toInt"},
		    {"LONG", "$toLong"},
		    {"DECIMAL", "$toDecimal"},
		    {"DOUBLE", "$toDouble"},
		    {"STRING", "$toString"},
		    {"DATE", "$toDate"},
		    {"BOOL", "$toBool"}
		};

		//<summary>
		//解析 ORDER BY 中的 CAST(expr AS type)，返回 MongoDB 表达式
		//</summary>
		private BsonValue ParseCastInOrderBy(string expr)
		{
		    var m = CastRegex.Match(expr);
		    if (!m.Success) return null;
		
		    string rawExpr = m.Groups[1].Value.Trim();
		    string targetType = m.Groups[2].Value.ToUpper();
		
		    string mongoOp; // 👈 提前声明
		    if (CastToMongo.TryGetValue(targetType, out mongoOp))
		    {
		        if (FieldRegex.IsMatch(rawExpr) && !rawExpr.StartsWith("$"))
		            rawExpr = "$" + rawExpr;
		
		        return BsonDocument.Parse(
		            string.Format("{{ \"{0}\": \"{1}\" }}", mongoOp, rawExpr));
		    }
		
		    return null;
		}
		
		//<summary>
		//返回 BsonDocument，供日志打印；外部再 .Sort(...) 即可
		//</summary>
		private BsonDocument BuildSortAuto(string orderBy)
		{
		    if (string.IsNullOrWhiteSpace(orderBy))
		        return null;
		
		    var doc = new BsonDocument();
		    foreach (var piece in orderBy.Split(','))
		    {
		        var tmp = piece.Trim().Split(' ');
		        string fld = tmp[0];
		        int  dir = (tmp.Length > 1 && tmp[1].ToUpper() == "DESC") ? -1 : 1;
		        
		        // 尝试解析 CAST
		        BsonValue sortKey = ParseCastInOrderBy(fld);
		        if (sortKey == null)
		        {
		            // 普通字段
		            sortKey = fld;
		        }
		        
		        // MongoDB 排序键必须是字符串或表达式
		        if (sortKey.IsBsonDocument)
		        {
		            // 表达式排序，使用 $project + $sort（仅聚合场景）
		            // 但 MongoDB 4.4+ 支持直接表达式排序
		            doc.Add(fld, new BsonDocument
		            {
		                { "$meta", "expression" }, // 占位，实际用表达式
		                { "value", sortKey },
		                { "direction", dir }
		            });
		        }
		        else
		        {
		            doc.Add(sortKey.AsString, dir);
		        }
		    }
		    // 日志里可拷
		    logger.Debug("[BuildSortAuto] 排序 JSON：" + doc.ToJson());
		    return doc;
		}
		#endregion


        #region ── 18. 参数替换（含脱敏）──
        private string ReplaceParameters(string text, IDictionary<string, object> paras)
        {
            if (text == null) return text;
            if (paras != null)
            {
                foreach (var kv in paras)
                {
                    object val = kv.Value;
                    string key = "@" + kv.Key;

                    if (val is IEnumerable && !(val is string))
                    {
                        text = text.Replace(key, JsonConvert.SerializeObject(val));
                        continue;
                    }

                    string jsonVal;
                    if (val == null) jsonVal = "null";
                    else if (val is string) jsonVal = "\"" + val.ToString().Replace("\"", "\\\"") + "\"";
                    else if (val is DateTime) jsonVal = "\"" + ((DateTime)val).ToString("o") + "\"";
                    else if (val is bool) jsonVal = val.ToString().ToLower();
                    else jsonVal = val.ToString();

                    text = text.Replace(key, jsonVal)
                               .Replace(":" + kv.Key, jsonVal);
                }
            }

            text = ObjectIdRegex.Replace(text, "\"$1\":ObjectId(\"$2\")");
            return text;
        }
        #endregion

        #region ── 19. BsonDocument→DataTable ──
        private static DataTable ToDataTable(List<BsonDocument> docs)
        {
            var dt = new DataTable();
            if (docs == null || docs.Count == 0)
            {
                logger.Debug("ToDataTable: 输入 docs 为 null 或 0 条，返回空表。");
                return dt;
            }

            var cols = new SortedSet<string>();
            foreach (var d in docs)
            {
                foreach (var e in d.Elements)
                {
                    if (e.Name == "_id" && e.Value.IsBsonDocument)
                    {
                        foreach (var sub in e.Value.AsBsonDocument)
                            cols.Add(sub.Name);
                    }
                    else
                    {
                        cols.Add(e.Name);
                    }
                }
            }

            foreach (var c in cols) dt.Columns.Add(c, typeof(object));
            logger.Debug("ToDataTable: 建列完成，列数=" + dt.Columns.Count);

            foreach (var d in docs)
            {
                DataRow r = dt.NewRow();
                foreach (var e in d.Elements)
                {
                    if (e.Name == "_id" && e.Value.IsBsonDocument)
                    {
                        foreach (var sub in e.Value.AsBsonDocument)
                            r[sub.Name] = BsonTypeMapper.MapToDotNetValue(sub.Value) ?? DBNull.Value;
                    }
                    else
                    {
                        r[e.Name] = BsonTypeMapper.MapToDotNetValue(e.Value) ?? DBNull.Value;
                    }
                }
                dt.Rows.Add(r);
            }

            logger.Debug("ToDataTable: 填行完成，行数=" + dt.Rows.Count);
            return dt;
        }
        #endregion

        #region ── 20. 缓存包装 ──
        private T CachedParse<T>(string sql, Func<string, T> parser) where T : class
        {
            var cached = _parseCache.Get(sql) as T;
            if (cached != null) return cached;

            var newValue = parser(sql);
            _parseCache.Set(sql, newValue, DateTimeOffset.UtcNow.AddHours(1));
            return newValue;
        }
        #endregion

        #region ── 21. 安全获取值 ──
        private static object SafeGetValue(BsonDocument doc, string fieldName)
        {
            if (doc == null) return null;
            BsonValue val;
            return doc.TryGetValue(fieldName, out val) ? BsonTypeMapper.MapToDotNetValue(val) : null;
        }
        #endregion
    }

    #region ── 统一异常 ──
    public sealed class MongoSqlException : Exception
    {
        public string Sql { get; private set; }
        public MongoSqlException(string message, string sql, Exception inner = null)
            : base(message + "  SQL=" + sql, inner) { Sql = sql; }
    }
    #endregion
}