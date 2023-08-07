using Npgsql;
using System;
using System.Configuration;
using System.Data;

namespace PGDAL
{
    public class PostGreContext
    {
        private static readonly log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly string PostGreConStr = ConfigurationManager.AppSettings["PostGreConStrDev"].ToString();
        private readonly string _queryStr = ConfigurationManager.AppSettings["PostGreQueryStr"];

        private static NpgsqlConnection GetConn()
        {
            return new NpgsqlConnection(PostGreConStr);
        }

        /// <summary>
        /// Connects to PostGre, selects entities and returns a DataTable.    
        /// </summary>
        /// <returns></returns>
        public DataTable SelectEntityRatingFromCl()
        {
            using (var con = GetConn())
            {
                Log.Debug($"Select from CL started at:  {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                var dt = new DataTable();
                var cmd = new NpgsqlCommand(_queryStr, con);
                var adapter = new NpgsqlDataAdapter(cmd);

                try
                {
                    adapter.Fill(dt);
                    Log.Debug($"SelectEntityRatingFromCl executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                }
                catch (Exception e)
                {
                    Log.Error("SelectEntityRatingFromCl failed : \n" + e.Message + "\n" + e.StackTrace);
                }
                return dt;
            }
        }

        public DataTable SelectIndustryCode()
        {
            using (var con = GetConn())
            {
                Log.Debug($"Select from CL started at:  {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                var dt = new DataTable();
                var myquery = @"select count(jsondoc_->>'EntityId'),jsondoc_->>'EntityId' as EntityId,jsondoc_->>'IndustryId' as IndustryId
                                from tenant.entityindustry                              
                                where  islatestversion_ and not isdeleted_
                                group by jsondoc_->>'EntityId',jsondoc_->>'IndustryId'
                                having count(jsondoc_->>'EntityId') >= 1";
                var cmd = new NpgsqlCommand(myquery, con);
                var adapter = new NpgsqlDataAdapter(cmd);
                //--where createddate_ <'2022-11-03 21:32:54.218463'
                try
                {
                    adapter.Fill(dt);
                    Log.Debug($"SelectEntityRatingFromCl executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                }
                catch (Exception e)
                {
                    Log.Error("SelectEntityRatingFromCl failed : \n" + e.Message + "\n" + e.StackTrace);
                }
                return dt;
            }
        }

        public DataTable SelectFinancials()
        {
            using (var con = GetConn())
            {
                Log.Debug($"Select from CL started at:  {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                var dt = new DataTable();
                var myquery = @"select jsondoc_->>'Id' as Id,
                                jsondoc_->>'EntityId' as EntityId  
                                from tenant.financial
                                where islatestversion_";
                var cmd = new NpgsqlCommand(myquery, con);
                var adapter = new NpgsqlDataAdapter(cmd);
                
                try
                {
                    adapter.Fill(dt);
                    Log.Debug($"SelectFinancials executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                }
                catch (Exception e)
                {
                    Log.Error("SelectFinancials failed : \n" + e.Message + "\n" + e.StackTrace);
                }
                return dt;
            }
        }
    }
}