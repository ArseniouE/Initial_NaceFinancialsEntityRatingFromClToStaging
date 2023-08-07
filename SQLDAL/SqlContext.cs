using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Text;
using System.Data;

namespace SQLDAL
{
    public class SqlContext
    {
        private static readonly log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly string _connectionString = ConfigurationManager.AppSettings["SQLConStrDev"];

        /// <summary>
        /// Connects to Sql, Truncates the Table, then Inserts the list of RatingScenarios from CL. Returns a string result 0 or 1 .
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public string SqlSequence(DataTable data)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                Log.Debug("Login to Sql Success.");
                try
                {
                    connection.Open();
                    TruncateTable(connection);
                    /*
                   --------Two Inserts Methods-------------
                   BulkInsert() provides fast insert for large number of data.
                   InsertToTable() for better data manipulation.
                   */
                    var result = BulkInsert(data, connection);
                    //InsertToTable(data, connection);
                    return result;
                }
                catch (Exception e)
                {
                    Log.Error("Login to Sql failed :\n" + e.Message + "\n" + e.StackTrace);
                    return "1";
                }
            }
        }

        public string SqlSequenceIndustryCode(DataTable data)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                Log.Debug("Login to Sql Success.");
                try
                {
                    connection.Open();
                    TruncateTableEntityIndustry(connection);
                    /*
                   --------Two Inserts Methods-------------
                   BulkInsert() provides fast insert for large number of data.
                   InsertToTable() for better data manipulation.
                   */
                    var result = BulkInsertIndustryCode(data, connection);
                    //InsertToTable(data, connection);
                    return result;
                }
                catch (Exception e)
                {
                    Log.Error("Login to Sql failed :\n" + e.Message + "\n" + e.StackTrace);
                    return "1";
                }
            }
        }

        public string SqlSequenceFinancials(DataTable data)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                Log.Debug("Login to Sql Success.");
                try
                {
                    connection.Open();
                    TruncateTableFinancials(connection);
                    /*
                   --------Two Inserts Methods-------------
                   BulkInsert() provides fast insert for large number of data.
                   InsertToTable() for better data manipulation.
                   */
                    var result = BulkInsertFinancials(data, connection);
                    //InsertToTable(data, connection);
                    return result;
                }
                catch (Exception e)
                {
                    Log.Error("Login to Sql failed :\n" + e.Message + "\n" + e.StackTrace);
                    return "1";
                }
            }
        }

        /// <summary>
        /// Truncates the Sql Table before Insert.
        /// </summary>
        /// <param name="connection"></param>
        private void TruncateTable(SqlConnection connection)
        {
            var queryStr = "TRUNCATE TABLE [ABRS_Staging].[dbo].[Initial_EntityRating]";
            
            using (var command = new SqlCommand(queryStr, connection))
            {
                try
                {
                    command.ExecuteNonQuery();
                    Log.Debug($"Truncate Table executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                }
                catch (Exception e)
                {
                    Log.Error($"Truncate Table failed :\n" + e.Message + "\n" + e.StackTrace);
                }
            }
        }

        private void TruncateTableEntityIndustry(SqlConnection connection)
        {
            var queryStr = "TRUNCATE TABLE [ABRS_Staging].[dbo].[ExistingIndustryCodes]";

            using (var command = new SqlCommand(queryStr, connection))
            {
                try
                {
                    command.ExecuteNonQuery();
                    Log.Debug($"Truncate Table executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                }
                catch (Exception e)
                {
                    Log.Error($"Truncate Table failed :\n" + e.Message + "\n" + e.StackTrace);
                }
            }
        }


        private void TruncateTableFinancials(SqlConnection connection)
        {
            var queryStr = "TRUNCATE TABLE [ABRS_Staging].[dbo].[Financials]";

            using (var command = new SqlCommand(queryStr, connection))
            {
                try
                {
                    command.ExecuteNonQuery();
                    Log.Debug($"Truncate Table executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                }
                catch (Exception e)
                {
                    Log.Error($"Truncate Table failed :\n" + e.Message + "\n" + e.StackTrace);
                }
            }
        }


        /// <summary>
        /// Bulk Insert for RatingScenarios DataTable to Sql Table
        /// </summary>
        /// <param name="data"></param>
        /// <param name="connection"></param>
        private static string BulkInsert(DataTable data, SqlConnection connection)
        {
            try
            {
                using (var copy = new SqlBulkCopy(connection))
                {
                    copy.DestinationTableName = "[ABRS_Staging].[dbo].[Initial_EntityRating]";

                    //Added column mappings so the column order doesn't matter.
                    copy.ColumnMappings.Add(data.Columns[0].ToString(), "Id");
                    copy.ColumnMappings.Add(data.Columns[1].ToString(), "ExpDate");
                    copy.ColumnMappings.Add(data.Columns[2].ToString(), "FinalPd");
                    copy.ColumnMappings.Add(data.Columns[3].ToString(), "ModelPd");
                    copy.ColumnMappings.Add(data.Columns[4].ToString(), "Approver");
                    copy.ColumnMappings.Add(data.Columns[5].ToString(), "Comments");
                    copy.ColumnMappings.Add(data.Columns[6].ToString(), "EntityId");
                    copy.ColumnMappings.Add(data.Columns[7].ToString(), "IsActive");
                    copy.ColumnMappings.Add(data.Columns[8].ToString(), "MasterPd");
                    copy.ColumnMappings.Add(data.Columns[9].ToString(), "SourcePd");
                    copy.ColumnMappings.Add(data.Columns[10].ToString(), "ApproveId");
                    copy.ColumnMappings.Add(data.Columns[11].ToString(), "CascadePd");
                    copy.ColumnMappings.Add(data.Columns[12].ToString(), "DefaultPd");
                    copy.ColumnMappings.Add(data.Columns[13].ToString(), "IsDefault");
                    copy.ColumnMappings.Add(data.Columns[14].ToString(), "OverlayPd");
                    copy.ColumnMappings.Add(data.Columns[15].ToString(), "FinalGrade");
                    copy.ColumnMappings.Add(data.Columns[16].ToString(), "FinalScore");
                    copy.ColumnMappings.Add(data.Columns[17].ToString(), "IsApproved");
                    copy.ColumnMappings.Add(data.Columns[18].ToString(), "ModelGrade");
                    copy.ColumnMappings.Add(data.Columns[19].ToString(), "OverridePd");
                    copy.ColumnMappings.Add(data.Columns[20].ToString(), "IsOutOfDate");
                    copy.ColumnMappings.Add(data.Columns[21].ToString(), "MasterGrade");
                    copy.ColumnMappings.Add(data.Columns[22].ToString(), "NextRevDate");
                    copy.ColumnMappings.Add(data.Columns[23].ToString(), "ApprovedDate");
                    copy.ColumnMappings.Add(data.Columns[24].ToString(), "ConfigVersion");
                    copy.ColumnMappings.Add(data.Columns[25].ToString(), "DefaultReason");
                    copy.ColumnMappings.Add(data.Columns[26].ToString(), "MasterFinalPd");
                    copy.ColumnMappings.Add(data.Columns[27].ToString(), "OverrideGrade");                   
                    copy.ColumnMappings.Add(data.Columns[28].ToString(), "ApprovalStatus");
                    copy.ColumnMappings.Add(data.Columns[29].ToString(), "CreditCommDate");
                    copy.ColumnMappings.Add(data.Columns[30].ToString(), "MasterSourcePd");
                    copy.ColumnMappings.Add(data.Columns[31].ToString(), "LatestApprovedScenarioId");
                    copy.ColumnMappings.Add(data.Columns[32].ToString(), "LatestApprovedRatingVersionId");
                    copy.ColumnMappings.Add(data.Columns[33].ToString(), "ModelId");

                    copy.WriteToServer(data);
                }
                Log.Debug($"Insert to STAGING executed successfully for {data.Rows.Count} ratingScenarios at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                return "0";
            }
            catch (Exception e)
            {
                Log.Error($"Insert to STAGING failed :\n" + e.Message + "\n" + e.StackTrace);
                return "1";
            }
        }

        private static string BulkInsertIndustryCode(DataTable data, SqlConnection connection)
        {
            try
            {
                using (var copy = new SqlBulkCopy(connection))
                {
                    copy.DestinationTableName = "[ABRS_Staging].[dbo].[ExistingIndustryCodes]";

                    //Added column mappings so the column order doesn't matter.
                    copy.ColumnMappings.Add(data.Columns[0].ToString(), "CodeCount");
                    copy.ColumnMappings.Add(data.Columns[1].ToString(), "EntityId");
                    copy.ColumnMappings.Add(data.Columns[2].ToString(), "IndustryId");
                    

                    copy.WriteToServer(data);
                }
                Log.Debug($"Insert to STAGING executed successfully for {data.Rows.Count} ratingScenarios at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                return "0";
            }
            catch (Exception e)
            {
                Log.Error($"Insert to STAGING failed :\n" + e.Message + "\n" + e.StackTrace);
                return "1";
            }
        }

        private static string BulkInsertFinancials(DataTable data, SqlConnection connection)
        {
            try
            {
                using (var copy = new SqlBulkCopy(connection))
                {
                    copy.DestinationTableName = "[ABRS_Staging].[dbo].[Financials]";

                    //Added column mappings so the column order doesn't matter.
                    copy.ColumnMappings.Add(data.Columns[0].ToString(), "Id");
                    copy.ColumnMappings.Add(data.Columns[1].ToString(), "EntityId");
                    


                    copy.WriteToServer(data);
                }
                Log.Debug($"Insert to STAGING executed successfully for {data.Rows.Count} financials at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                return "0";
            }
            catch (Exception e)
            {
                Log.Error($"Insert to financials failed :\n" + e.Message + "\n" + e.StackTrace);
                return "1";
            }
        }



        /// <summary>
        /// Inserts the List of RatingScenarios from CL to SQL table.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="connection"></param>
        private static void InsertToTable(DataTable data, SqlConnection connection)
        {
            var insertedCount = 0;
            foreach (DataRow ratingScenario in data.Rows)
            {
                var queryStr = CreateQueryString(ratingScenario);
                using (var command = new SqlCommand(queryStr, connection))
                {
                    try
                    {
                        command.ExecuteNonQuery();
                        insertedCount++;
                    }
                    catch (Exception e)
                    {
                        Log.Error($"Insert to STAGING failed for EntityRating:{ratingScenario[0]}:\n" + e.Message + "\n" + e.StackTrace);
                    }
                }
            }
            Log.Debug($"Insert to STAGING executed successfully for: {insertedCount} of total: {data.Rows.Count} EntityRating at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
        }

        /// <summary>
        /// Prepares the insert query.
        /// </summary>
        /// <param name="ratingScenario"></param>
        /// <returns></returns>
        private static string CreateQueryString(DataRow ratingScenario)
        {
            var insertQuery = new StringBuilder();
            insertQuery.Append("INSERT INTO [ABRS_Staging].[dbo].[RatingScenarioFromCl] Values");

            insertQuery.Append($"('{ratingScenario[0]}',NULLIF('{ratingScenario[1]}',''),NULLIF('{ratingScenario[2]}',''),NULLIF('{ratingScenario[3]}',''),NULLIF('{ratingScenario[4]}',''),NULLIF('{ratingScenario[5]}',''),NULLIF('{ratingScenario[6]}','')," +
                               $"NULLIF('{ratingScenario[7]}',''),{ratingScenario[8]},NULLIF('{ratingScenario[9]}',''),NULLIF('{ratingScenario[10]}',''),NULLIF('{ratingScenario[11]}',''),NULLIF('{ratingScenario[12]}','')," +
                               $"NULLIF('{ratingScenario[13]}',''),NULLIF('{ratingScenario[14]}',''),NULLIF('{ratingScenario[15]}',''),NULLIF('{ratingScenario[16]}',''),NULLIF('{ratingScenario[17]}',''),NULLIF('{ratingScenario[18]}','')," +
                               $"NULLIF('{ratingScenario[19]}',''),NULLIF('{ratingScenario[20]}',''),NULLIF('{ratingScenario[21]}',''),NULLIF('{ratingScenario[22]}',''),NULLIF('{ratingScenario[23]}',''),NULLIF('{ratingScenario[24]}','')," +
                               $"NULLIF('{ratingScenario[25]}',''),NULLIF('{ratingScenario[26]}',''),NULLIF('{ratingScenario[27]}',''),NULLIF('{ratingScenario[28]}',''),NULLIF('{ratingScenario[29]}',''),NULLIF('{ratingScenario[30]}','')," +
                               $"NULLIF('{ratingScenario[31]}',''),NULLIF('{ratingScenario[32]}',''),NULLIF('{ratingScenario[33]}',''),NULLIF('{ratingScenario[34]}',''),NULLIF('{ratingScenario[35]}',''),NULLIF('{ratingScenario[36]}','')," +
                               $"NULLIF('{ratingScenario[37]}',''),NULLIF('{ratingScenario[38]}',''),NULLIF('{ratingScenario[39]}',''),NULLIF('{ratingScenario[40]}',''),NULLIF('{ratingScenario[41]}',''))");
            return insertQuery.ToString();
        }
    }
}
