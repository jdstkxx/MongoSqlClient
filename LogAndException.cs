// ===================================================================================
//  MongoSqlClient  C# 5.0 兼容完整版 
//  封装类：LogAndException
//  说明：抛异常日志，自动记error日志，并且翻译系统错误信息成用户能理解的错误信息。
//  依赖：log4net类
// ===================================================================================
using System;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using log4net;

namespace MongoSql.Demo
{
    public static class LogAndException
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(LogAndException));

        //<summary>
        //      LogAndException.DebugRaw = false; // true代表调试期，直接裸抛，默认False不裸抛
        //      LogAndException.ThrowTech = false;   // 在调试期DebugRaw=false才生效，True返回[技术出错信息]，False返回翻译过的[用户出错信息]，默认False
        //</summary>
        public static bool DebugRaw { get; set; }
        public static bool ThrowTech { get; set; }
        
        //默认值
        static LogAndException()
        {
            DebugRaw = false;   // true代表调试期，直接裸抛，默认False不裸抛
            ThrowTech = false;   // 在调试期DebugRaw=false才生效，True返回[技术出错信息]，False返回翻译过的[用户出错信息]，默认False
        }


        public static void Throw(Exception original, string mes = null)
		{
		    if (original == null) 
		    	throw new ArgumentNullException("original Exception is null ...");
		
		    string user = GetUserMessage(original);
		    string tech = GetTechMessage(original);
		    string loc  = GetMyCodeLocation(original);
		
			//记录详细堆栈日志
	    	logger.Error(mes,original);
	    	//记录精简日志
	        logger.Error(string.Format("LOC={0} \n MSG={1} \n USER={2} \n TECH={3}",
	                                    loc, mes ?? "", user, tech));
		
		    if (DebugRaw)
		    	// 裸抛原始错误信息
		        throw original;
		    else	
		    	// 抛用户友好错误提示
		    	throw new Exception(user, original);
		}
        
        public static string GetUserMessage(Exception ex)
        {
            if (IsConnectTimeout(ex))
                return "网络繁忙，请稍后重试；若仍无法使用，请联系客服。";

            SqlException sqlex = ex as SqlException;
            if (sqlex != null)
            {
                if (sqlex.Number == 18456) return "登录失败，请联系管理员确认账号密码。";
                if (sqlex.Number == 4060 || sqlex.Number == 233 || sqlex.Number == 2)
                    return "系统维护中，请稍后再试。";
                if (sqlex.Number == 1205) return "操作冲突，请稍后重试。";
            }


            return "系统繁忙，请稍后重试。";
        }

        public static string GetTechMessage(Exception ex)
        {
            SqlException sqlex = ex as SqlException;
            if (sqlex != null)
                return string.Format("(SQL错误号:{0} | 服务器:{1}) 消息:{2}",
                                      sqlex.Number, sqlex.Server, sqlex.Message);

            Win32Exception winex = ex as Win32Exception;
            if (winex != null)
            {
                uint code = (uint)winex.NativeErrorCode;
                return string.Format("(Win32错误码:0x{0:X8}) 消息:{1}", code, winex.Message);
            }

            return string.Format("其他异常类型:{0} | 消息:{1}",
                                  ex.GetType().Name, ex.Message);
        }


		
		private static string GetMyCodeLocation(Exception ex)
		{
			Exception root = ex;
            while (root.InnerException != null)
                root = root.InnerException;
            
		    if (root == null) return "定位出错行失败，传入ex为null";
		
		    var st = new StackTrace(root, true);
		    if (st == null) return "定位出错行失败，StackTrace(ex, true)返回null)";   // 理论上不会出现，但防御
		
		    var frames = st.GetFrames();
		    if (frames == null) return "定位出错行失败，st.GetFrames()返回null)";		

            
		    foreach (var frame in frames)
		    {
		        if (frame == null) continue;                 // 单帧可能为null
		
		        string file = frame.GetFileName();
		        if (!string.IsNullOrEmpty(file))
		        {
		            return string.Format("定位出错信息成功:{0} at {1} in {2}:{3}",
		                                 ex.GetType().Name,
		                                 frame.GetMethod() == null ? "未知方法" : frame.GetMethod().Name,
		                                 Path.GetFileName(file),
		                                 frame.GetFileLineNumber());
		        }
		    }

		    return "定位出错行失败，可能是(无PDB)";
		}		

        private static bool IsConnectTimeout(Exception ex)
        {
            SqlException sqlex = ex as SqlException;
            if (sqlex != null)
                return sqlex.Number == -2 || sqlex.Number == 258;

            Win32Exception winex = ex as Win32Exception;
            if (winex != null)
                return (uint)winex.NativeErrorCode == 258u;

            return false;
        }
    }
}